package public

import (
	"encoding/json"
	"net/http"
	"strings"

	"decentralized-api/observability"
	"decentralized-api/utils"

	"github.com/labstack/echo/v4"
)

const completionsPath = "/v1/completions"

func (s *Server) postCompletions(ctx echo.Context) error {
	_, requestOp := startObservabilityInferenceRequestContext(ctx)
	var spanErr error
	defer requestOp.FinishErr(&spanErr)

	body, err := readRequestBody(ctx.Request(), ctx.Response().Writer)
	if err != nil {
		chatErr := mapRequestBodyReadError(err)
		spanErr = observability.Error.Fmt(err, "read request body: %v", chatErr)
		return chatErr
	}

	var completionsReq CompletionsRequest
	if err := json.Unmarshal(body, &completionsReq); err != nil {
		// return echo.NewHTTPError(http.StatusBadRequest, "invalid request format")
		spanErr = observability.Error.Fmt(err, "unmarshal completions request body: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request format: %v", err)
	}

	if strings.TrimSpace(completionsReq.Model) == "" {
		httpErr := echo.NewHTTPError(http.StatusBadRequest, "model is required")
		spanErr = observability.Error.Fmt(httpErr, "model is required in completions request")
		return httpErr
	}
	if len(completionsReq.Prompt) == 0 {
		httpErr := echo.NewHTTPError(http.StatusBadRequest, "prompt is required")
		spanErr = observability.Error.Fmt(httpErr, "prompt is required in completions request")
		return httpErr
	}
	if len(completionsReq.Prompt) > 1 {
		httpErr := echo.NewHTTPError(http.StatusBadRequest, "batch prompts are not supported")
		spanErr = observability.Error.Fmt(httpErr, "batch prompts are not supported in completions request")
		return httpErr
	}
	for _, prompt := range completionsReq.Prompt {
		if strings.TrimSpace(prompt) == "" {
			httpErr := echo.NewHTTPError(http.StatusBadRequest, "prompt is required")
			spanErr = observability.Error.Fmt(httpErr, "prompt is required in completions request")
			return httpErr
		}
	}

	// Use the common request pipeline without local proxy recursion.
	// Signature is always validated against the original /v1/completions body.
	signBodyHash := utils.GenerateSHA256Hash(string(body))
	responseErr := s.postChatWithBody(ctx, requestOp, body, signBodyHash, completionsPath, body)
	if responseErr != nil {
		spanErr = observability.Error.Fmt(responseErr, "post completion with body: %v", responseErr)
		return responseErr
	}
	return nil
}

func tryBuildOpenAiRequestFromCompletionsBody(body []byte) (OpenAiRequest, bool) {
	var completionsReq CompletionsRequest
	if err := json.Unmarshal(body, &completionsReq); err != nil {
		return OpenAiRequest{}, false
	}
	if strings.TrimSpace(completionsReq.Model) == "" || len(completionsReq.Prompt) != 1 {
		return OpenAiRequest{}, false
	}

	rawPrompt := completionsReq.Prompt.First()
	if strings.TrimSpace(rawPrompt) == "" {
		return OpenAiRequest{}, false
	}

	var maxTokens int32
	if completionsReq.MaxTokens != nil {
		maxTokens = *completionsReq.MaxTokens
	}
	var seed int32
	if completionsReq.Seed != nil {
		seed = *completionsReq.Seed
	}
	promptText := rawPrompt

	return OpenAiRequest{
		Model:               completionsReq.Model,
		Seed:                seed,
		MaxTokens:           maxTokens,
		MaxCompletionTokens: maxTokens,
		Messages: []Message{{
			Role:    "user",
			Content: MessageContent{Text: &promptText},
		}},
	}, true
}
