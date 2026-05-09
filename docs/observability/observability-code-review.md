# Observability Code Review

Review scope: staged observability changes as of 2026-05-12.  
All findings resolved on 2026-05-12.

Validation performed:

- `go build ./...` passed in `decentralized-api`.
- `go build ./...` passed in `devshard`.
- `jq empty deploy/join/observability/grafana/dashboards/*.json` passed.
- `gofmt -l` clean after formatting pass.

## Findings

### 1. Setup report metrics are passive cache reads and can be missing or stale ✓ fixed

Severity: High

`setupreportprovider.NewProvider` only reads `adminserver.GetCachedReport()` and returns `nil` if the admin report cache has not been populated yet. The cache is populated only through `/admin/v1/setup/report`; Prometheus scraping the metrics endpoint does not generate or refresh the report. After a report is generated once, `GetCachedReport()` also returns it without checking `CachedUntil`, so metrics can remain stale if the admin endpoint is not called again.

References:

- [`setupreportprovider/provider.go`](../../decentralized-api/observability/setupreportprovider/provider.go#L14-L19)
- [`setup_report.go`](../../decentralized-api/internal/server/admin/setup_report.go#L80-L108)

Impact: the Network Details dashboard may show no setup-report data after startup, or old setup/block-sync values after the cache TTL has expired.

Recommendation: move report generation/TTL refresh behind a server-owned provider, or run a periodic setup-report refresh owned by the admin server. At minimum, expose `generated_at` / `cached_until` metrics so stale dashboards are obvious.

### 2. Setup report metric help text still claims optional checks are excluded ✓ fixed

Severity: Medium

The setup report collector comments and Prometheus help strings say optional checks are excluded. Current backend behavior counts every failed check in `FailedChecks` and uses every failure to compute `OverallStatus`.

References:

- [`collector.go`](../../decentralized-api/observability/setupreport/collector.go#L25)
- [`collector.go`](../../decentralized-api/observability/setupreport/collector.go#L49)
- [`collector.go`](../../decentralized-api/observability/setupreport/collector.go#L74)
- [`setup_report.go`](../../decentralized-api/internal/server/admin/setup_report.go#L803-L865)

Impact: Prometheus metadata and future alerting rules can encode the wrong semantics.

Recommendation: update the collector comments/help strings to say the metrics reflect the setup report exactly. If optional behavior is desired, keep it as a separate Grafana presentation layer or add explicit optional-aware metrics.

### 3. Network Details says `validator_in_set` does not affect overall status, but backend includes it ✓ fixed

Severity: Medium

The Network Details dashboard visually treats `validator_in_set` as optional. That visual treatment is fine, but the panel description currently says it does not affect overall status. The backend setup report still includes all failures in `OverallStatus`, including `validator_in_set`.

References:

- [`network-details.json`](../../deploy/join/observability/grafana/dashboards/network-details.json#L420-L437)
- [`setup_report.go`](../../decentralized-api/internal/server/admin/setup_report.go#L853-L865)

Impact: operators can see an orange optional tile while the Overall Status tile is red, with dashboard text implying that should not happen.

Recommendation: change the panel description to something like: `Visual-only optional context; backend setup report status still includes this check.`

### 4. Go formatting is not clean ✓ fixed

Severity: Low

`gofmt -l` reports:

- `decentralized-api/observability/init.go`
- `devshard/observability/init.go`
- `devshard/observability/metrics_prometheus.go`

Impact: formatting-only diff noise and possible CI style failures if gofmt is enforced.

Recommendation: run `gofmt` on the changed Go files before merging.

### 5. `BuildProviders` should clean up the trace exporter if metric exporter creation fails ✓ fixed

Severity: Low

`BuildProviders` creates the trace exporter first, then creates the metric exporter. If metric exporter creation fails, the trace exporter is returned neither to the caller nor shut down.

Reference:

- [`init.go`](../../decentralized-api/observability/init.go#L61-L68)

Impact: startup failure path can leak exporter resources. It is narrow, but easy to avoid.

Recommendation: call `traceExporter.Shutdown(ctx)` before returning the metric exporter error, or create both exporters behind a cleanup helper.

### 6. Devshard success-rate panel can divide by zero when there is no traffic ✓ fixed

Severity: Low

The Devshard Details success-rate query divides completed rate by total inference rate. During quiet periods the denominator can be zero, producing no data or `NaN` depending on Prometheus/Grafana behavior.

Reference:

- [`devshard-details.json`](../../deploy/join/observability/grafana/dashboards/devshard-details.json#L61)

Impact: empty or confusing KPI during idle periods.

Recommendation: guard the query with a denominator fallback or display an explicit idle value.

### 7. Dashboard schema versions are inconsistent ✓ fixed

Severity: Low

`network-details.json` uses `schemaVersion: 39`, while the new/consolidated dashboards use Grafana `schemaVersion: 42`.

Reference:

- [`network-details.json`](../../deploy/join/observability/grafana/dashboards/network-details.json#L560)

Impact: likely harmless, but it makes dashboard provisioning harder to compare and review.

Recommendation: normalize dashboard schema versions when exporting/provisioning the final dashboard set.

## Positive Notes

- The collector/provider split for setup report metrics matches the participant metrics architecture and avoids pulling application dependencies into collector packages.
- The new devshard inference metrics line up with the Devshard Details dashboard queries.
- Dashboard JSON is syntactically valid.
- The consolidated dashboard set is simpler: Participant Details, Network Details, and Devshard Details replace several overlapping older dashboards.
