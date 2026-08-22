package filters

import (
	stdjson "encoding/json"
	"sort"
	"strconv"
	"strings"
)

const maxParameterBytes = 4 << 10

func ParametersOf(body []byte) string {
	var fields map[string]stdjson.RawMessage
	if stdjson.Unmarshal(body, &fields) != nil {
		return ""
	}
	delete(fields, "messages")
	delete(fields, "prompt")

	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)

	var rendered strings.Builder
	rendered.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			rendered.WriteByte(',')
		}
		rendered.WriteString(strconv.Quote(name))
		rendered.WriteByte(':')
		rendered.Write(fields[name])
		if rendered.Len() > maxParameterBytes {
			return rendered.String()[:maxParameterBytes] + "…}"
		}
	}
	rendered.WriteByte('}')
	return rendered.String()
}
