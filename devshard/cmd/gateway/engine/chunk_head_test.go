package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A host that answers a 49 000-token prompt returns the ids the gateway asked for and strips again, so the
// chunk that would show a delta is hundreds of kilobytes of numbers the log must not spend its budget on.
func TestTheHeadElidesTheIdsThatMakeAContentlessChunkLarge(t *testing.T) {
	ids := make([]string, 0, 49_019)
	for id := range 49_019 {
		ids = append(ids, fmt.Sprint(id))
	}
	chunk := fmt.Appendf(nil,
		`data: {"choices":[{"delta":{"content":""},"finish_reason":"length","token_ids":[7,8,9]}],"prompt_token_ids":[%s],"usage":{"completion_tokens":0,"prompt_tokens":49019}}`,
		strings.Join(ids, ","))
	require.Greater(t, len(chunk), 200_000, "the fixture has to be the size that made the field useless")

	head := chunkHead(chunk)

	require.Contains(t, head, `"delta":{"content":""}`, "the delta is what the head exists to show")
	require.Contains(t, head, `"finish_reason":"length"`)
	require.Contains(t, head, `"prompt_token_ids":[...]`)
	require.Contains(t, head, `"token_ids":[...]`)
	require.Contains(t, head, `"completion_tokens":0`, "the usage sits past the ids and must survive them")
	require.LessOrEqual(t, len(head), maxEmptyChunkLogged+len(elidedValue))
}

// A nested value is skipped by its own brackets alone, or the head would resume inside the array it elided.
func TestTheHeadResumesAfterANestedBulkyValue(t *testing.T) {
	chunk := []byte(`data: {"prompt_logprobs":[{"7":{"logprob":-0.5,"top":[1,2]}},null],"usage":{"prompt_tokens":3}}`)

	head := chunkHead(chunk)

	require.Equal(t, `data: {"prompt_logprobs":[...],"usage":{"prompt_tokens":3}}`, head)
}

// A bulky name with a value the log can read is left alone: "logprobs":null says something, [...] does not.
func TestTheHeadKeepsABulkyFieldThatIsNotAnArray(t *testing.T) {
	chunk := []byte(`data: {"choices":[{"logprobs":null,"delta":{"content":"hi"}}]}`)

	head := chunkHead(chunk)

	require.Equal(t, `data: {"choices":[{"logprobs":null,"delta":{"content":"hi"}}]}`, head)
}

// A brace inside a string is not a bracket, or a prompt containing one would cut the skip short.
func TestTheHeadSkipsPastBracketsInsideStrings(t *testing.T) {
	chunk := []byte(`data: {"token_ids":[{"text":"]}\"still inside"}],"usage":{"prompt_tokens":1}}`)

	head := chunkHead(chunk)

	require.Equal(t, `data: {"token_ids":[...],"usage":{"prompt_tokens":1}}`, head)
}

// The terminator carries no answer, so a chunk that is only [DONE] must not become the head that is kept.
func TestTheTerminatorIsNeverKeptAsAHead(t *testing.T) {
	require.Empty(t, chunkHead([]byte("data: [DONE]\n\n")))
}
