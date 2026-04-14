package observability

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestInitInstruments_ReinitializesOnMeterProviderChange(t *testing.T) {
	oldProvider := otel.GetMeterProvider()
	oldInstrumentProvider := instrumentProvider
	t.Cleanup(func() {
		otel.SetMeterProvider(oldProvider)
		instrumentProvider = oldInstrumentProvider
	})

	provider1 := sdkmetric.NewMeterProvider()
	provider2 := sdkmetric.NewMeterProvider()
	instrumentProvider = nil

	otel.SetMeterProvider(provider1)
	initInstruments()
	require.Same(t, provider1, instrumentProvider)

	otel.SetMeterProvider(provider2)
	initInstruments()
	require.Same(t, provider2, instrumentProvider)
}