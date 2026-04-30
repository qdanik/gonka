package promsd

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync/atomic"
)

type targetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels,omitempty"`
}

func Handler(routes *atomic.Value, target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		routeMap, _ := routes.Load().(map[string]string)
		versions := make([]string, 0, len(routeMap))
		for version := range routeMap {
			versions = append(versions, version)
		}
		sort.Strings(versions)

		groups := make([]targetGroup, 0, len(versions))
		for _, version := range versions {
			groups = append(groups, targetGroup{
				Targets: []string{target},
				Labels: map[string]string{
					"__metrics_path__": "/" + version + "/metrics",
					"version":          version,
					"service":          "devshardd",
				},
			})
		}

		if err := json.NewEncoder(w).Encode(groups); err != nil {
			http.Error(w, "failed to encode service discovery", http.StatusInternalServerError)
		}
	}
}