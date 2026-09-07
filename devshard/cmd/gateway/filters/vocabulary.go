package filters

// The strings a cacheability answer puts into the record, one per reason a reply may not be stored.
const (
	CacheStorable          = ""
	CacheRefusedEmptyBody  = "empty_body"
	CacheRefusedFailure    = "transient_failure"
	CacheRefusedUnreadable = "unreadable_event"
	CacheRefusedUnfinished = "unfinished_answer"
	CacheRefusedStatus     = "unreplayable_status"
)

// How a host names a failure of the moment rather than of the request: in prose, and as a class in `type`
// or in a `code` that is not a status. A class is matched with its separators removed, so it lists only
// what no phrase above already spells. See README.md, "Cacheability".
var (
	momentaryFailureMessages = []string{
		"context canceled",
		"context cancelled",
		"client disconnected",
		"request canceled",
		"request cancelled",
		"timeout",
		"timed out",
		"rate limit",
		"overloaded",
		"temporarily unavailable",
		"service unavailable",
		"internal server error",
		"unsupported model",
		"model not found",
		"model_not_found",
		"does not exist",
		"not supported on this model",
	}

	momentaryFailureClasses = []string{
		"server",
		"toomanyrequests",
		"notfound",
		"connection",
		"quota",
		"cancel",
		"disconnect",
	}
)
