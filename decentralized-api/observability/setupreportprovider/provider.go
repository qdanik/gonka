// Package setupreportprovider wires the admin setup report into the Prometheus
// collector by adapting *adminserver.SetupReport → *setupreportobs.ReportSnapshot.
// It follows the same pattern as observability/participantprovider.
package setupreportprovider

import (
	"strconv"
	"strings"
	"time"

	"decentralized-api/chainphase"
	adminserver "decentralized-api/internal/server/admin"
	setupreportobs "decentralized-api/observability/setupreport"
)

// NewProvider returns a function suitable for setupreportobs.SetProvider.
//
// tracker is used to populate BlockHeight and SecondsSinceBlock directly from
// the live ChainPhaseTracker — which is updated push-based on every new block
// via the WebSocket event stream — so those metrics are always current without
// any additional polling or RPC calls.
//
// Check statuses (OverallStatus, PassedChecks, etc.) still come from the
// cached setup report which is refreshed periodically by StartPeriodicRefresh.
func NewProvider(tracker *chainphase.ChainPhaseTracker) func() *setupreportobs.ReportSnapshot {
	return func() *setupreportobs.ReportSnapshot {
		report := adminserver.GetCachedReport()
		if report == nil {
			return nil
		}

		snap := &setupreportobs.ReportSnapshot{
			PassedChecks:      report.Summary.PassedChecks,
			FailedChecks:      report.Summary.FailedChecks,
			UnavailableChecks: report.Summary.UnavailableChecks,
			Checks:            make(map[string]float64, len(report.Checks)),
		}

		switch report.OverallStatus {
		case adminserver.PASS:
			snap.OverallStatus = setupreportobs.StatusPass
		case adminserver.UNAVAILABLE:
			snap.OverallStatus = setupreportobs.StatusUnavailable
		default:
			snap.OverallStatus = setupreportobs.StatusFail
		}

		for _, c := range report.Checks {
			switch c.Status {
			case adminserver.PASS:
				snap.Checks[c.ID] = setupreportobs.StatusPass
			case adminserver.UNAVAILABLE:
				snap.Checks[c.ID] = setupreportobs.StatusUnavailable
			default:
				snap.Checks[c.ID] = setupreportobs.StatusFail
			}

			details, _ := c.Details.(map[string]interface{})

			switch c.ID {
			case "cold_key_configured":
				snap.ColdKeyAddress = strVal(details, "address")
				snap.ColdKeyPubkey = strVal(details, "pubkey")

			case "permissions_granted":
				snap.PermissionsGranted = int64(numVal(details, "granted"))
				snap.PermissionsMissing = int64(numVal(details, "missing"))

			case "feegrant_allowance":
				snap.FeegrantAllowanceType = strVal(details, "allowance_type")
				snap.FeegrantColdKey = strVal(details, "cold_key_address")
				snap.FeegrantWarmKey = strVal(details, "warm_key_address")
				snap.FeegrantSpendLimit = strVal(details, "spend_limit")
				if exp := strVal(details, "expiration"); exp != "" {
					if t, err := time.Parse(time.RFC3339, exp); err == nil {
						snap.FeegrantExpiration = float64(t.Unix())
					}
				}

			case "active_in_epoch":
				snap.EpochNumber = int64(numVal(details, "epoch"))
				snap.EpochWeight = int64(numVal(details, "weight"))

			case "missed_requests_threshold":
				snap.MissedPercentage = numVal(details, "missed_percentage")
				snap.MissedRequests = int64(numVal(details, "missed_requests"))
				snap.TotalRequests = int64(numVal(details, "total_requests"))
				snap.InferenceCount = int64(numVal(details, "inference_count"))
			}

			// Parse GPU data from mlnode_* checks.
			if strings.HasPrefix(c.ID, "mlnode_") && details != nil {
				nodeID := strVal(details, "id")
				if nodeID == "" {
					nodeID = strings.TrimPrefix(c.ID, "mlnode_")
				}
				if gpus, ok := details["gpus"].([]adminserver.GPUDeviceInfo); ok {
					for _, gpu := range gpus {
						stat := setupreportobs.GPUStat{
							NodeID:     nodeID,
							GPUIndex:   strconv.Itoa(gpu.Index),
							Name:       gpu.Name,
							TotalMemGB: gpu.TotalMemoryGB,
							FreeMemGB:  gpu.FreeMemoryGB,
							UsedMemGB:  gpu.UsedMemoryGB,
							Available:  boolToFloat64(gpu.Available),
							Utilization: -1,
							Temperature: -1,
						}
						if gpu.Utilization != nil {
							stat.Utilization = float64(*gpu.Utilization)
						}
						if gpu.Temperature != nil {
							stat.Temperature = float64(*gpu.Temperature)
						}
						snap.GPUStats = append(snap.GPUStats, stat)
					}
				}
			}
		}

		// Block height and lag come from ChainPhaseTracker — push-based, updated
		// on every block via the WebSocket stream, no RPC call needed.
		if tracker != nil {
			if st := tracker.GetCurrentEpochState(); st != nil {
				snap.BlockHeight = st.CurrentBlock.Height
				if !st.CurrentBlock.Time.IsZero() {
					snap.SecondsSinceBlock = time.Since(st.CurrentBlock.Time).Seconds()
				}
			}
		}

		return snap
	}
}

// strVal extracts a string value from a map[string]interface{}.
func strVal(m map[string]interface{}, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

// numVal extracts a float64 value from a map[string]interface{}.
// JSON numbers decoded into interface{} are always float64.
func numVal(m map[string]interface{}, key string) float64 {
	if m == nil {
		return 0
	}
	v, _ := m[key].(float64)
	return v
}

// boolToFloat64 converts a bool to 1.0 or 0.0.
func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
