package filters

import "testing"

// The pipeline answers this from the document it already decoded. An unparsable body has no case here
// any more: it fails the pipeline before a rule runs, where it used to reach a second unmarshal.
func TestRequiresToolsReadsTheDecodedDocument(t *testing.T) {
	testCases := []struct {
		name string
		body string
		want bool
	}{
		{name: "no tools", body: `{"model":"qwen"}`},
		{name: "empty tools", body: `{"tools":[]}`},
		{name: "tools present", body: `{"tools":[{"type":"function"}]}`, want: true},
		{name: "tool_choice none", body: `{"tool_choice":"none"}`},
		{name: "tool_choice auto", body: `{"tool_choice":"auto"}`, want: true},
		{name: "tool_choice object", body: `{"tool_choice":{"type":"function"}}`, want: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			document, err := ParseDocument([]byte(testCase.body))
			if err != nil {
				t.Fatalf("ParseDocument(): %v", err)
			}
			if got := requiresTools(document); got != testCase.want {
				t.Fatalf("got %v, want %v", got, testCase.want)
			}
		})
	}
}
