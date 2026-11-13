import asyncio
import json
import socket
import time
from types import SimpleNamespace

import httpx
import msgpack
import pytest
import zmq

import api.kv_events as kv
import api.proxy as proxy_module
from api.inference.vllm import runner as runner_mod
from api.inference.vllm.runner import VLLMRunner

BS = 4
TOKENS = list(range(1, 17))  # four full blocks of 4
PORTS = [5001, 5002, 5003]
KEY_A = "a" * 64


def stored(hashes, tokens, size=BS, parent=None, **extra):
    return {
        "type": "BlockStored", "block_hashes": hashes, "parent_block_hash": parent,
        "token_ids": tokens, "block_size": size, "lora_id": None, "medium": "GPU",
        "lora_name": None, **extra,
    }


def removed(hashes, **extra):
    return {"type": "BlockRemoved", "block_hashes": hashes, "medium": "GPU", **extra}


def frames(*events, seq=0):
    """The publisher's wire format: (topic, 8-byte big-endian seq, msgpack [ts, events, dp])."""
    return [b"", seq.to_bytes(8, "big"), msgpack.packb([1.0, list(events), 0])]


def held(indexes, port, tokens=TOKENS):
    return indexes.match_tokens(port, indexes.request_keys(tokens))


@pytest.fixture
def indexes(monkeypatch):
    fresh = kv.KvIndexes()
    fresh.indexes = {p: kv.InstanceIndex() for p in PORTS}
    monkeypatch.setattr(kv, "indexes", fresh)
    return fresh


# --- event decoding and chained keys ---------------------------------------


def test_stored_batch_indexes_leading_blocks(indexes):
    """A msgpack BlockStored batch makes the instance hold the request's leading blocks."""
    indexes._handle(5001, frames(stored([b"h1", b"h2", b"h3"], TOKENS[:12], extra_keys=[None] * 3)), None)
    assert held(indexes, 5001) == 12
    assert held(indexes, 5002) == 0


def test_chain_keys_depend_on_parent_and_tokens():
    keys = kv.chain_keys(TOKENS, BS)
    assert len(keys) == 4 and len(set(keys)) == 4
    assert keys[0] == kv.block_key(b"", TOKENS[:4])
    assert keys[1] == kv.block_key(keys[0], TOKENS[4:8])
    assert kv.chain_keys(TOKENS[:6], BS) == keys[:1]  # only full blocks
    assert kv.chain_keys([9] + TOKENS[1:], BS)[1] != keys[1]  # the first block feeds the second


def test_stored_events_chain_through_parent_hash(indexes):
    """A later event continues the chain by parent hash; an unknown parent is ignored."""
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)
    indexes._handle(5001, frames(stored([b"h2"], TOKENS[4:8], parent=b"h1"), seq=1), 0)
    indexes._handle(5001, frames(stored([b"h9"], TOKENS[12:16], parent=b"missing"), seq=2), 1)
    assert held(indexes, 5001) == 8
    assert len(indexes.indexes[5001]) == 2


def test_removed_stops_the_run_and_cleared_empties(indexes):
    indexes._handle(5001, frames(stored([b"h1", b"h2", b"h3"], TOKENS[:12])), None)
    indexes._handle(5001, frames(removed([b"h2"]), seq=1), 0)
    assert held(indexes, 5001) == 4  # h3 is still there but the run breaks at h2
    indexes._handle(5001, frames({"type": "AllBlocksCleared"}, seq=2), 1)
    assert held(indexes, 5001) == 0 and len(indexes.indexes[5001]) == 0


def test_duplicate_store_then_one_removal_leaves_nothing(indexes):
    event = stored([b"h1"], TOKENS[:4])
    indexes._handle(5001, frames(event, event), None)
    indexes._handle(5001, frames(removed([b"h1"]), seq=1), 0)
    assert len(indexes.indexes[5001]) == 0 and indexes.indexes[5001]._refs == {}


def test_cpu_medium_events_are_ignored(indexes):
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)
    indexes._handle(5001, frames({**removed([b"h1"]), "medium": "CPU"}, {**stored([b"c"], TOKENS[4:8]), "medium": "CPU"}, seq=1), 0)
    assert held(indexes, 5001) == 4


def test_partial_entries_and_extra_keys_are_not_indexed(indexes):
    indexes._handle(5001, frames(stored([b"h1", b"h2"], TOKENS[:8])), None)
    indexes._handle(5001, frames(stored([b"p"], TOKENS[8:10], size=2, parent=b"h2"), seq=1), 0)  # partial
    indexes._handle(5001, frames(stored([b"m", b"n"], TOKENS[8:16], parent=b"h2", extra_keys=[("mm",), None]), seq=2), 1)
    assert len(indexes.indexes[5001]) == 2 and held(indexes, 5001) == 8


def test_index_is_bounded_and_rebuilds_after_overflow(indexes):
    small = kv.InstanceIndex(max_blocks=2)
    small.apply(stored([b"h1", b"h2", b"h3"], TOKENS[:12]))
    assert len(small) == 0  # over the cap: cleared, not left with orphaned children
    small.apply(stored([b"h1"], TOKENS[:4]))
    assert len(small) == 1


def test_seq_gap_or_restart_resets_the_index(indexes):
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)
    assert held(indexes, 5001) == 4
    indexes._handle(5001, frames(stored([b"h2"], TOKENS[4:8], parent=b"h1"), seq=5), 0)  # gap
    assert held(indexes, 5001) == 0  # h1 forgotten, so h2's parent was unknown


def test_bad_message_resets_and_does_not_raise(indexes):
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)
    assert indexes._handle(5001, [b"", (1).to_bytes(8, "big"), b"\xc1not msgpack"], 0) is None
    assert held(indexes, 5001) == 0


def test_scores_compare_tokens_across_block_sizes(indexes):
    big = TOKENS[:16]
    indexes._handle(5001, frames(stored([b"a1", b"a2"], big, size=8)), None)  # 16 tokens in 2 blocks
    indexes._handle(5002, frames(stored([b"b1", b"b2", b"b3"], big[:12])), None)  # 12 tokens in 3 blocks
    assert held(indexes, 5001) == 16 and held(indexes, 5002) == 12


# --- picking ----------------------------------------------------------------


@pytest.fixture
def three_instances(monkeypatch, indexes):
    monkeypatch.setattr(proxy_module, "vllm_healthy", {p: True for p in PORTS})
    monkeypatch.setattr(proxy_module, "vllm_counts", {p: 0 for p in PORTS})
    monkeypatch.setattr(proxy_module, "vllm_prefix_index", {})
    return indexes


@pytest.mark.asyncio
async def test_pick_prefers_the_instance_holding_the_longer_reported_prefix(three_instances):
    winner = max(PORTS, key=lambda p: proxy_module._rendezvous_score(KEY_A, p))
    longer, shorter = [p for p in PORTS if p != winner]
    three_instances._handle(longer, frames(stored([b"x1", b"x2", b"x3"], TOKENS[:12])), None)
    three_instances._handle(shorter, frames(stored([b"y1"], TOKENS[:4])), None)
    keys = three_instances.request_keys(TOKENS)
    assert await proxy_module._pick_vllm_backend(KEY_A, None, keys) == longer
    # Nothing reported for the request: rendezvous decides.
    assert await proxy_module._pick_vllm_backend(KEY_A, None, three_instances.request_keys(TOKENS[::-1])) == winner


@pytest.mark.asyncio
async def test_pick_reported_prefix_only_counts_within_the_lead_bound(three_instances):
    three_instances._handle(5001, frames(stored([b"x1", b"x2"], TOKENS[:8])), None)
    proxy_module.vllm_counts.update({5001: 9, 5002: 0, 5003: 1})
    assert await proxy_module._pick_vllm_backend(None, None, three_instances.request_keys(TOKENS)) == 5002


@pytest.mark.asyncio
async def test_pick_without_request_keys_uses_the_approximate_index(three_instances):
    hashes = [i.to_bytes(8, "little") for i in (1, 2)]
    first = await proxy_module._pick_vllm_backend(None, hashes, None)
    await proxy_module._release_vllm_backend(first)
    assert await proxy_module._pick_vllm_backend(None, hashes, None) == first


# --- request side: tokenize and fallback --------------------------------------


def _request(body: bytes):
    async def read():
        return body
    return SimpleNamespace(method="POST", body=read)


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


CHAT = json.dumps({"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": True, "max_tokens": 5}).encode()


@pytest.mark.asyncio
async def test_request_keys_come_from_tokenize_on_the_least_loaded_instance(monkeypatch, indexes):
    monkeypatch.setenv("INFERENCE_KV_EVENTS", "1")
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)
    seen = []

    def handler(request):
        seen.append((request.url.port, request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"tokens": TOKENS, "count": len(TOKENS)})

    async with _client(handler) as client:
        keys = await kv.request_prefix_keys(
            _request(CHAT), "v1/chat/completions", client, {5001: True, 5002: True}, {5001: 3, 5002: 1}
        )
    assert seen == [(5002, "/tokenize", {"model": "m", "messages": [{"role": "user", "content": "hi"}]})]
    assert keys == {BS: kv.chain_keys(TOKENS, BS)}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["status", "timeout", "garbage", "empty", "connect"])
async def test_tokenize_failure_falls_back(monkeypatch, indexes, failure):
    monkeypatch.setenv("INFERENCE_KV_EVENTS", "1")
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)

    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("slow", request=request)
        if failure == "connect":
            raise httpx.ConnectError("down", request=request)
        if failure == "status":
            return httpx.Response(404)
        if failure == "empty":
            return httpx.Response(200, json={"tokens": []})
        return httpx.Response(200, content=b"not json")

    async with _client(handler) as client:
        assert await kv.request_prefix_keys(_request(CHAT), "/v1/chat/completions", client, {5001: True}, {5001: 0}) is None


@pytest.mark.asyncio
async def test_no_keys_when_nothing_reported_yet_or_body_unusable(monkeypatch, indexes):
    monkeypatch.setenv("INFERENCE_KV_EVENTS", "1")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"tokens": TOKENS})

    image = json.dumps({"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {}}]}]}).encode()
    async with _client(handler) as client:
        for body in (CHAT, b"{broken", image):
            assert await kv.request_prefix_keys(_request(body), "/v1/chat/completions", client, {5001: True}, {5001: 0}) is None
    assert len(calls) == 1  # CHAT was tokenized, but no instance had reported a block size yet


@pytest.mark.asyncio
async def test_out_of_range_prompt_token_ids_fall_back(monkeypatch, indexes):
    monkeypatch.setenv("INFERENCE_KV_EVENTS", "1")
    indexes._handle(5001, frames(stored([b"h1"], TOKENS[:4])), None)
    body = json.dumps({"model": "m", "prompt": [1, -2, 3]}).encode()
    async with _client(lambda request: httpx.Response(500)) as client:
        assert await kv.request_prefix_keys(_request(body), "/v1/completions", client, {5001: True}, {5001: 0}) is None


@pytest.mark.asyncio
async def test_switch_off_skips_the_body_and_tokenize(monkeypatch, indexes):
    monkeypatch.delenv("INFERENCE_KV_EVENTS", raising=False)
    boom = SimpleNamespace(method="POST", body=lambda: pytest.fail("body read with the switch off"))
    assert await kv.request_prefix_keys(boom, "/v1/chat/completions", object(), {5001: True}, {5001: 0}) is None


# --- runner launch args --------------------------------------------------------


def _launch(monkeypatch, instances):
    commands = []
    process = type("P", (), {"poll": staticmethod(lambda: None)})()
    runner = VLLMRunner(model="dummy-model")
    monkeypatch.setattr(runner_mod.torch.cuda, "device_count", lambda: instances)
    monkeypatch.setattr(runner, "_verify_and_fix_env", lambda: None)
    monkeypatch.setattr(runner, "_wait_for_server", lambda: True)
    monkeypatch.setattr(runner_mod, "setup_vllm_proxy", lambda ports: None)
    monkeypatch.setattr(runner_mod.subprocess, "Popen", lambda command, **kw: commands.append(command[2]) or process)
    runner.start()
    return commands


def test_switch_off_launch_args_have_no_kv_events(monkeypatch):
    monkeypatch.delenv("INFERENCE_KV_EVENTS", raising=False)
    assert not any("kv-events" in c for c in _launch(monkeypatch, 2))


def test_switch_on_gives_each_instance_its_own_endpoint(monkeypatch):
    monkeypatch.setenv("INFERENCE_KV_EVENTS", "true")
    commands = _launch(monkeypatch, 2)
    assert "tcp://127.0.0.1:5557" in commands[0] and "tcp://127.0.0.1:5558" in commands[1]
    assert "--kv-events-config" in commands[0] and "tcp://127.0.0.1:5558" not in commands[0]
    assert json.loads(kv.kv_events_config_json(1)) == {
        "enable_kv_cache_events": True, "publisher": "zmq", "endpoint": "tcp://127.0.0.1:5558",
    }


# --- live ZMQ ------------------------------------------------------------------


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_subscriber_receives_events_from_a_connecting_publisher(monkeypatch):
    """vLLM's PUB connects to the proxy's bound SUB; a batch reaches the index, stop() frees it."""
    port = _free_port()
    monkeypatch.setenv("INFERENCE_KV_EVENTS_BASE_PORT", str(port))
    live = kv.KvIndexes()
    live.sync([5001], generation=1)
    pub = zmq.Context.instance().socket(zmq.PUB)
    pub.connect(kv.kv_events_endpoint(0))
    payload = frames(stored([b"h1", b"h2"], TOKENS[:8]))
    deadline = time.monotonic() + 5
    try:
        while live.match_tokens(5001, live_keys := {BS: kv.chain_keys(TOKENS, BS)}) != 8:
            assert time.monotonic() < deadline, "no BlockStored batch reached the index within 5s"
            pub.send_multipart(payload)
            await asyncio.sleep(0.05)
        assert live.match_tokens(5001, live_keys) == 8
    finally:
        pub.close(linger=0)
        await live.stop()
    assert live.indexes == {}
