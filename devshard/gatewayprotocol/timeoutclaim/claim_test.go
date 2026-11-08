package timeoutclaim

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"devshard/host"
	"devshard/user"
)

type recordedCall struct {
	method          string
	ctx             context.Context
	nonce           uint64
	sendTime        time.Time
	payload         *host.InferencePayload
	finishTx        []byte
	responsePayload []byte
}

type recordingHandler struct {
	calls []recordedCall
}

var (
	errTimeoutVote = errors.New("timeout vote")
	errMissClaim   = errors.New("miss claim")
	timeoutResult  = user.TimeoutResult{Reason: "execution", Outcome: "applied", Applied: true}
	missResult     = user.TimeoutResult{Reason: "error", Outcome: "applied", Applied: true, ResponseHash: []byte("response-hash")}
)

func (h *recordingHandler) HandleTimeout(ctx context.Context, nonce uint64, sendTime time.Time, payload *host.InferencePayload) (user.TimeoutResult, error) {
	h.calls = append(h.calls, recordedCall{method: "HandleTimeout", ctx: ctx, nonce: nonce, sendTime: sendTime, payload: payload})
	return timeoutResult, errTimeoutVote
}

func (h *recordingHandler) HandleErrorMiss(ctx context.Context, nonce uint64, finishTx, responsePayload []byte) (user.TimeoutResult, error) {
	h.calls = append(h.calls, recordedCall{method: "HandleErrorMiss", ctx: ctx, nonce: nonce, finishTx: finishTx, responsePayload: responsePayload})
	return missResult, errMissClaim
}

type contextKey struct{}

// Test flow:
//  1. Build a recording handler, a tagged context, a nonce, a send time, a payload and a response payload.
//  2. Define a table of Finish transactions, varying across a non-empty one, nil and an empty non-nil one.
//  3. For each case, call `Resolve` with the case's Finish.
//  4. Assert exactly one call was made, `HandleErrorMiss` with the Finish and the response payload when the Finish is non-empty and `HandleTimeout` with the send time and the payload otherwise, every argument passed through and the handler's result and error returned.
func TestResolveClaimsAMissExactlyWhenAFinishIsGiven(t *testing.T) {
	ctx := context.WithValue(context.Background(), contextKey{}, "resolve")
	const nonce = uint64(42)
	sendTime := time.Unix(1_700_000_000, 0)
	payload := &host.InferencePayload{Prompt: []byte("prompt"), Model: "qwen", InputLength: 6, MaxTokens: 64, StartedAt: 1_700_000_000}
	responsePayload := []byte(`{"events":["data: {}"]}`)
	finishTx := []byte("finish-tx")
	cases := []struct {
		name       string
		finishTx   []byte
		want       recordedCall
		wantResult user.TimeoutResult
		wantErr    error
	}{
		{
			name:       "a Finish claims the miss",
			finishTx:   finishTx,
			want:       recordedCall{method: "HandleErrorMiss", ctx: ctx, nonce: nonce, finishTx: finishTx, responsePayload: responsePayload},
			wantResult: missResult,
			wantErr:    errMissClaim,
		},
		{
			name:       "no Finish votes the timeout",
			want:       recordedCall{method: "HandleTimeout", ctx: ctx, nonce: nonce, sendTime: sendTime, payload: payload},
			wantResult: timeoutResult,
			wantErr:    errTimeoutVote,
		},
		{
			name:       "an empty Finish votes the timeout",
			finishTx:   []byte{},
			want:       recordedCall{method: "HandleTimeout", ctx: ctx, nonce: nonce, sendTime: sendTime, payload: payload},
			wantResult: timeoutResult,
			wantErr:    errTimeoutVote,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			handler := &recordingHandler{}

			result, err := Resolve(ctx, handler, nonce, sendTime, payload, testCase.finishTx, responsePayload)

			if !reflect.DeepEqual(handler.calls, []recordedCall{testCase.want}) {
				t.Errorf("calls = %+v, want [%+v]", handler.calls, testCase.want)
			}
			if !reflect.DeepEqual(result, testCase.wantResult) {
				t.Errorf("result = %+v, want %+v", result, testCase.wantResult)
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Errorf("err = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}
