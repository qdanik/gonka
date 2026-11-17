// Package sessionload drives multi-turn conversations through a gateway: each turn resends the whole conversation, so the prompt grows and a host's prefix cache can pay off.
package sessionload

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"devshard/testenv/mockopenai"
)

// Config is one run: Sessions conversations run at once, each ends after Turns turns and a fresh one starts until Duration is over.
type Config struct {
	URL           string
	APIKey        string
	Model         string
	Seed          uint64
	Sessions      int
	Turns         int
	Think         time.Duration
	CacheKeyShare float64
	SystemBytes   int
	UserBytes     int
	MaxTokens     int
	Duration      time.Duration
	RequestLimit  time.Duration
	MaxRetries    int
}

// Turn is the outcome of one request of a conversation.
type Turn struct {
	Conversation string        `json:"conversation"`
	Fingerprint  string        `json:"fingerprint"`
	Index        int           `json:"index"`
	Keyed        bool          `json:"keyed"`
	StartedAt    time.Time     `json:"started_at"`
	TTFT         time.Duration `json:"ttft_ns"`
	Latency      time.Duration `json:"latency_ns"`
	Status       int           `json:"status"`
	Retries      int           `json:"retries"`
	PromptBytes  int           `json:"prompt_bytes"`
	Error        string        `json:"error,omitempty"`
}

// CancelledAtRunEnd is the Error of a turn the run ended on while it waited to retry; it is neither a served turn nor a failure of the system under test.
const CancelledAtRunEnd = "cancelled before retry"

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Run returns every turn of every conversation once Duration is over and the turns in flight have finished.
func Run(ctx context.Context, cfg Config) []Turn {
	ctx, stop := context.WithTimeout(ctx, cfg.Duration)
	defer stop()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: cfg.Sessions}}
	var (
		mu    sync.Mutex
		turns []Turn
		wg    sync.WaitGroup
	)
	for slot := 0; slot < cfg.Sessions; slot++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !sleep(ctx, startDelay(cfg, slot)) {
				return
			}
			for round := 0; ctx.Err() == nil; round++ {
				done := conversation(ctx, client, cfg, slot, round)
				mu.Lock()
				turns = append(turns, done...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return turns
}

// startDelay staggers the first conversations over half a conversation's length, so the run does not open with every session cold at once.
func startDelay(cfg Config, slot int) time.Duration {
	rng := rand.New(rand.NewPCG(cfg.Seed, ^uint64(slot)))
	return time.Duration(rng.Float64() * float64(cfg.Turns) * float64(cfg.Think) / 2)
}

// conversation seeds its own generator from the run seed, slot and round, so a scenario replays the same prompts whatever the timing.
func conversation(ctx context.Context, client *http.Client, cfg Config, slot, round int) []Turn {
	rng := rand.New(rand.NewPCG(cfg.Seed, uint64(slot)<<32|uint64(round)))
	id := fmt.Sprintf("s%d-%d-%d", cfg.Seed, slot, round)
	keyed := rng.Float64() < cfg.CacheKeyShare
	history := []message{{Role: "system", Content: filler(rng, id, cfg.SystemBytes)}}
	var turns []Turn
	for index := 0; index < cfg.Turns && ctx.Err() == nil; index++ {
		history = append(history, message{Role: "user", Content: filler(rng, fmt.Sprintf("%s-q%d", id, index), cfg.UserBytes)})
		turn, answer := send(ctx, client, cfg, id, index, keyed, history)
		turns = append(turns, turn)
		if turn.Error != "" {
			break
		}
		history = append(history, message{Role: "assistant", Content: answer})
		if !sleep(ctx, time.Duration(float64(cfg.Think)*(0.5+rng.Float64()))) {
			break
		}
	}
	return turns
}

func filler(rng *rand.Rand, tag string, size int) string {
	var b strings.Builder
	b.WriteString(tag)
	for b.Len() < size {
		b.WriteByte(byte('a' + rng.IntN(26)))
	}
	return b.String()
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// send retries 429 and 5xx up to MaxRetries; a turn that ends in an error ends its conversation.
func send(ctx context.Context, client *http.Client, cfg Config, id string, index int, keyed bool, history []message) (Turn, string) {
	payload := map[string]any{"model": cfg.Model, "stream": true, "max_tokens": cfg.MaxTokens, "messages": history}
	if keyed {
		payload["prompt_cache_key"] = id
	}
	body, _ := json.Marshal(payload)
	turn := Turn{Conversation: id, Fingerprint: mockopenai.ConversationFingerprint(history[0].Role, history[0].Content), Index: index, Keyed: keyed, StartedAt: time.Now(), PromptBytes: len(body)}
	for attempt := 0; ; attempt++ {
		answer, status, ttft, err := attemptOnce(ctx, client, cfg, body, turn.StartedAt)
		turn.Status, turn.TTFT = status, ttft
		if err == nil && status == http.StatusOK {
			turn.Latency = time.Since(turn.StartedAt)
			return turn, answer
		}
		retryable := err == nil && (status == http.StatusTooManyRequests || status >= 500)
		if !retryable || attempt >= cfg.MaxRetries {
			turn.Latency = time.Since(turn.StartedAt)
			turn.Error = fmt.Sprintf("status %d: %v", status, err)
			return turn, ""
		}
		turn.Retries++
		if !sleep(ctx, 200*time.Millisecond) {
			turn.Error = CancelledAtRunEnd
			return turn, ""
		}
	}
}

// attemptOnce streams one answer and times the first non-empty content delta from the turn's first send.
func attemptOnce(ctx context.Context, client *http.Client, cfg Config, body []byte, started time.Time) (answer string, status int, ttft time.Duration, err error) {
	reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.RequestLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, cfg.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return "", resp.StatusCode, 0, nil
	}
	var text strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for scanner.Scan() {
		data, ok := strings.CutPrefix(scanner.Text(), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil || len(chunk.Choices) == 0 {
			continue
		}
		if content := chunk.Choices[0].Delta.Content; content != "" {
			if ttft == 0 {
				ttft = time.Since(started)
			}
			text.WriteString(content)
		}
	}
	return text.String(), resp.StatusCode, ttft, scanner.Err()
}
