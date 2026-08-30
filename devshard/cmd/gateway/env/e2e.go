package env

import (
	"time"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/internal/e2econfig"
	"devshard/logging"
)

// E2E is what a declared end-to-end stand may shorten. See docs/rules.md, "What a test stand may reach".
type E2E struct {
	StreamingHardTimeout time.Duration
}

// A refused value is dropped rather than returned: it is inert by design, and must not stop a boot.
func LoadE2E() E2E {
	streamingHardTimeout, err := e2econfig.DurationMillisFromEnv(e2econfig.StreamingHardTimeoutMillisEnv)
	if err != nil {
		logging.Warn("end-to-end override ignored", logkey.Subsystem, "env",
			logkey.Recorded, e2econfig.StreamingHardTimeoutMillisEnv, logkey.Error, err)
		return E2E{}
	}
	return E2E{StreamingHardTimeout: streamingHardTimeout}
}
