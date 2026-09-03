package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/config"
)

// A missing dependency used to be a panic on the first request, or inside the constructor itself.
func TestNewEngineNamesTheDependencyItWasNotGiven(t *testing.T) {
	_, err := NewEngine(Deps{})

	require.ErrorContains(t, err, "Config")
}

func TestNewEngineRefusesEveryRequiredDependencyByName(t *testing.T) {
	for _, missing := range []string{"Picker", "Targets", "Windows", "Perf", "Snapshots"} {
		t.Run(missing, func(t *testing.T) {
			deps := completeEngineDeps(t)
			switch missing {
			case "Picker":
				deps.Picker = nil
			case "Targets":
				deps.Targets = nil
			case "Windows":
				deps.Windows = nil
			case "Perf":
				deps.Perf = nil
			case "Snapshots":
				deps.Snapshots = nil
			}

			_, err := NewEngine(deps)

			require.ErrorContains(t, err, missing)
		})
	}
}

// The optional ones stay optional: every use of them is already guarded.
func TestNewEngineAcceptsTheOptionalDependenciesUnset(t *testing.T) {
	engine, err := NewEngine(completeEngineDeps(t))

	require.NoError(t, err)
	require.NotNil(t, engine)
}

func completeEngineDeps(t *testing.T) Deps {
	t.Helper()
	sim := newSimulator(t, EscalationPolicy{}, 3, "qwen")
	return Deps{
		Picker:    sim.picker,
		Targets:   sim.target,
		Windows:   sim.windows,
		Perf:      sim.perf,
		Snapshots: sim.snapshots,
		Config:    config.NewHolder(engineSettings(EscalationPolicy{}, config.Modes{})),
	}
}
