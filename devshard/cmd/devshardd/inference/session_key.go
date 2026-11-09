package inference

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"devshard"
)

func sessionKey(body []byte) string {
	var req struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	for i, raw := range req.Messages {
		var msg struct {
			Role string `json:"role"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			return ""
		}
		if msg.Role != "user" {
			continue
		}
		opening, err := json.Marshal(req.Messages[:i+1])
		if err != nil {
			return ""
		}
		canonical, err := devshard.CanonicalizeJSON(opening)
		if err != nil {
			return ""
		}
		sum := sha256.Sum256(canonical)
		return hex.EncodeToString(sum[:])
	}
	return ""
}
