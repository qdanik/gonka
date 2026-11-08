package streamevidence

import (
	"encoding/json"

	"common/completionapi"
)

// Payload serializes evidence lines into the response payload of an error-miss claim, whose hash a verifier compares with the hashes in the Finish transaction.
func Payload(lines []string) ([]byte, error) {
	return json.Marshal(completionapi.SerializedStreamedResponse{Events: lines})
}
