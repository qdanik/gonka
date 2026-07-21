package perf

// inputBucket buckets by input-token count: 0:<1k 1:1k-5k 2:5k-15k 3:15k-30k 4:30k-100k 5:>=100k.
func inputBucket(tokens uint64) int {
	switch {
	case tokens < 1_000:
		return 0
	case tokens < 5_000:
		return 1
	case tokens < 15_000:
		return 2
	case tokens < 30_000:
		return 3
	case tokens < 100_000:
		return 4
	default:
		return 5
	}
}
