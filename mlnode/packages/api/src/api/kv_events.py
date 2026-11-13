"""Route by the prefix each vLLM instance reports holding in its KV cache.

vLLM (--kv-events-config) publishes BlockStored / BlockRemoved / AllBlocksCleared over ZMQ.
Its block hashes cannot be reproduced outside the process, so the index is keyed by our own
chained hash over the event's token ids. Off unless INFERENCE_KV_EVENTS is set.
"""
import asyncio
import hashlib
import json
import os
import struct
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx

from common.logger import create_logger

logger = create_logger(__name__)

# Hard cap on (group, vLLM hash) entries per instance, on top of what vLLM itself reports.
MAX_INDEX_BLOCKS = 200_000
# Longest request prefix scored, in blocks.
MAX_REQUEST_BLOCKS = 1024
SUBSCRIBER_RETRY_SECONDS = 1.0
_TOKENIZE_FIELDS = (
    "model", "messages", "prompt", "tools", "chat_template", "chat_template_kwargs",
    "add_generation_prompt", "continue_final_message", "add_special_tokens", "documents",
)
_PROMPT_PATHS = ("/v1/chat/completions", "/v1/completions")


def kv_events_enabled() -> bool:
    return os.getenv("INFERENCE_KV_EVENTS", "").strip().lower() in ("1", "true", "yes", "on")


def kv_events_endpoint(instance: int) -> str:
    """Endpoint of the i-th instance. vLLM connects to it (no '*' in the address, so its PUB
    socket does not bind), the proxy binds it."""
    try:
        base = int(os.getenv("INFERENCE_KV_EVENTS_BASE_PORT", "5557"))
    except ValueError:
        base = 5557
    return f"tcp://127.0.0.1:{base + instance}"


def kv_events_config_json(instance: int) -> str:
    return json.dumps({
        "enable_kv_cache_events": True,
        "publisher": "zmq",
        "endpoint": kv_events_endpoint(instance),
    })


def _tokenize_timeout() -> float:
    try:
        return max(0.05, float(os.getenv("INFERENCE_KV_EVENTS_TOKENIZE_TIMEOUT", "0.5")))
    except ValueError:
        return 0.5


def block_key(parent: bytes, tokens: Iterable[int]) -> bytes:
    """Chained key of one block: hash of the parent key and the block's token ids."""
    tokens = list(tokens)
    return hashlib.blake2b(parent + struct.pack(f"<{len(tokens)}I", *tokens), digest_size=16).digest()


def chain_keys(tokens: List[int], block_size: int) -> List[bytes]:
    """Keys of the full leading blocks of `tokens` (at most MAX_REQUEST_BLOCKS)."""
    keys: List[bytes] = []
    parent = b""
    for start in range(0, min(len(tokens) // block_size, MAX_REQUEST_BLOCKS) * block_size, block_size):
        parent = block_key(parent, tokens[start:start + block_size])
        keys.append(parent)
    return keys


class InstanceIndex:
    """What one vLLM instance reports holding, as chained block keys.

    A group (KV cache group; several on hybrid-attention models) has one canonical block
    size, the largest reported: smaller sizes are partial-block entries and are ignored.
    """

    def __init__(self, max_blocks: int = MAX_INDEX_BLOCKS):
        self._max_blocks = max_blocks
        self._keys: Dict[Tuple[int, Any], bytes] = {}  # (group, vLLM hash) -> our key
        self._refs: Dict[bytes, int] = {}  # our key -> number of (group, hash) holding it
        self.group_sizes: Dict[int, int] = {}

    def __len__(self) -> int:
        return len(self._keys)

    def clear(self) -> None:
        self._keys.clear()
        self._refs.clear()
        self.group_sizes.clear()

    def block_sizes(self) -> set:
        return set(self.group_sizes.values())

    def apply(self, event: Dict[str, Any]) -> None:
        if event.get("medium") not in (None, "GPU"):
            return
        kind = event.get("type")
        if kind == "BlockStored":
            self._store(event)
        elif kind == "BlockRemoved":
            group = event.get("group_idx") or 0
            for h in event.get("block_hashes") or ():
                self._drop((group, h))
        elif kind == "AllBlocksCleared":
            self.clear()

    def _store(self, event: Dict[str, Any]) -> None:
        group = event.get("group_idx") or 0
        hashes, tokens, size = event["block_hashes"], event["token_ids"], event["block_size"]
        if not isinstance(size, int) or size <= 0 or len(tokens) != len(hashes) * size:
            return  # masked blocks in a hybrid group, or malformed
        known = self.group_sizes.get(group, 0)
        if size < known:
            return
        if size > known:
            self._drop_group(group)
            self.group_sizes[group] = size
        if event.get("parent_block_hash") is None:
            parent = b""
        else:
            parent = self._keys.get((group, event["parent_block_hash"]))
            if parent is None:
                return  # parent unknown (evicted, or stored before our subscription)
        extra = event.get("extra_keys") or ()
        for n, h in enumerate(hashes):
            if n < len(extra) and extra[n] is not None:
                return  # multimodal / LoRA / cache salt: never matches a text-only key
            parent = block_key(parent, tokens[n * size:(n + 1) * size])
            if (group, h) not in self._keys:
                self._keys[(group, h)] = parent
                self._refs[parent] = self._refs.get(parent, 0) + 1
        if len(self._keys) > self._max_blocks:
            self.clear()  # evicting roots would leave unmatched orphans; rebuild from new events

    def _drop(self, entry: Tuple[int, Any]) -> None:
        key = self._keys.pop(entry, None)
        if key is None:
            return
        if self._refs[key] <= 1:
            del self._refs[key]
        else:
            self._refs[key] -= 1

    def _drop_group(self, group: int) -> None:
        for entry in [e for e in self._keys if e[0] == group]:
            self._drop(entry)

    def match_tokens(self, request_keys: Dict[int, List[bytes]]) -> int:
        """Longest leading run of the request's blocks held, in tokens, over the groups."""
        best = 0
        for size in self.block_sizes():
            run = 0
            for key in request_keys.get(size, ()):
                if key not in self._refs:
                    break
                run += 1
            best = max(best, run * size)
        return best


class KvIndexes:
    """Per-instance indexes plus one ZMQ subscriber task per instance."""

    def __init__(self):
        self.indexes: Dict[int, InstanceIndex] = {}
        self._tasks: Dict[int, asyncio.Task] = {}
        self._generation: Optional[int] = None

    def sync(self, ports: List[int], generation: int) -> None:
        """Call from the event loop. One subscriber per port (the i-th port uses the i-th
        endpoint); a new generation (vLLM restart) drops every index and subscriber."""
        retiring: List[asyncio.Task] = []
        if generation != self._generation:
            retiring = self._cancel_all()
            self._generation = generation
            self.indexes = {p: InstanceIndex() for p in ports}
        for i, port in enumerate(ports):
            task = self._tasks.get(port)
            if task is None or task.done():
                self._tasks[port] = asyncio.create_task(self._subscribe(i, port, retiring))

    def reset(self, port: int) -> None:
        index = self.indexes.get(port)
        if index is not None:
            index.clear()

    def request_keys(self, tokens: List[int]) -> Dict[int, List[bytes]]:
        sizes = set().union(*(i.block_sizes() for i in self.indexes.values()))
        return {size: chain_keys(tokens, size) for size in sizes}

    def match_tokens(self, port: int, request_keys: Dict[int, List[bytes]]) -> int:
        index = self.indexes.get(port)
        return index.match_tokens(request_keys) if index else 0

    def _cancel_all(self) -> List[asyncio.Task]:
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        self._tasks = {}
        return tasks

    async def stop(self) -> None:
        tasks = self._cancel_all()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.indexes = {}
        self._generation = None

    async def _subscribe(self, instance: int, port: int, retiring: List[asyncio.Task]) -> None:
        import zmq
        import zmq.asyncio

        endpoint = kv_events_endpoint(instance)
        # The previous generation's sockets must be closed before the endpoint can be bound again.
        await asyncio.gather(*retiring, return_exceptions=True)
        while True:
            sock = None
            try:
                sock = zmq.asyncio.Context.instance().socket(zmq.SUB)
                sock.setsockopt(zmq.SUBSCRIBE, b"")
                sock.bind(endpoint)
                self.reset(port)
                logger.info("KV events: listening for %d on %s", port, endpoint)
                last_seq: Optional[int] = None
                while True:
                    last_seq = self._handle(port, await sock.recv_multipart(), last_seq)
                    await asyncio.sleep(0)  # a queued backlog must not starve the event loop
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("KV events subscriber for %d on %s failed: %s", port, endpoint, exc)
            finally:
                if sock is not None:
                    sock.close(linger=0)
            self.reset(port)
            await asyncio.sleep(SUBSCRIBER_RETRY_SECONDS)

    def _handle(self, port: int, frames: List[bytes], last_seq: Optional[int]) -> Optional[int]:
        """Apply one (topic, seq, payload) message; an unexpected seq or payload resets the
        instance first, since the index no longer follows vLLM's cache."""
        import msgpack

        try:
            seq = int.from_bytes(frames[1], "big")
            if last_seq is None or seq != last_seq + 1:
                self.reset(port)
            index = self.indexes.get(port)
            if index is not None:
                for event in msgpack.unpackb(frames[2], raw=False, strict_map_key=False)[1]:
                    index.apply(event)
            return seq
        except Exception as exc:
            logger.warning("KV events: bad message from %d, index reset: %s", port, exc)
            self.reset(port)
            return None


indexes = KvIndexes()


def is_prompt_request(method: str, path: str) -> bool:
    return method == "POST" and "/" + path.lstrip("/") in _PROMPT_PATHS


def _valid_tokens(tokens: Any) -> bool:
    """Non-empty list of ids that fit the key packing."""
    return isinstance(tokens, list) and bool(tokens) and all(
        isinstance(t, int) and 0 <= t < 2**32 for t in tokens
    )


def _text_only(messages: Any) -> bool:
    if not isinstance(messages, list):
        return False
    for message in messages:
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list) and any(
            not isinstance(part, dict) or part.get("type") != "text" for part in content
        ):
            return False
    return True


async def request_token_ids(client: httpx.AsyncClient, port: int, body: bytes) -> Optional[List[int]]:
    """The request's token ids from that instance's own /tokenize, or None on any problem
    (unparsable body, non-text content, error, timeout) so the caller falls back."""
    try:
        data = json.loads(body)
        if not isinstance(data, dict):
            return None
        if "messages" in data:
            if not _text_only(data["messages"]):
                return None
        else:
            prompt = data.get("prompt")
            if isinstance(prompt, list) and _valid_tokens(prompt):
                return prompt
            if not isinstance(prompt, str):
                return None
        payload = {k: v for k, v in data.items() if k in _TOKENIZE_FIELDS}
        resp = await client.post(
            f"http://127.0.0.1:{port}/tokenize", json=payload, timeout=_tokenize_timeout()
        )
        if resp.status_code != 200:
            return None
        tokens = resp.json().get("tokens")
        if _valid_tokens(tokens):
            return tokens
    except Exception as exc:
        logger.debug("KV events: tokenize failed, using the approximate index: %s", exc)
    return None


async def request_prefix_keys(
    request: Any,
    path: str,
    client: Optional[httpx.AsyncClient],
    healthy: Dict[int, bool],
    counts: Dict[int, int],
) -> Optional[Dict[int, List[bytes]]]:
    """Block keys of the request's tokens, tokenized on the least loaded healthy instance.

    None when events are off, the request is not a prompt request, or anything fails. Reads
    the whole body; Starlette replays it to the upstream.
    """
    if not kv_events_enabled() or not is_prompt_request(request.method, path):
        return None
    live = [p for p, ok in healthy.items() if ok]
    if not live or client is None:
        return None
    try:
        tokens = await request_token_ids(client, min(live, key=lambda p: counts.get(p, 0)), await request.body())
        keys = indexes.request_keys(tokens) if tokens else {}
    except Exception as exc:
        logger.debug("KV events: could not build request keys: %s", exc)
        return None
    # No instance has reported yet (empty): keep the approximate index in charge.
    return keys or None
