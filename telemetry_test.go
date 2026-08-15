package xorbie

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/kadtest"
	"github.com/iand/xorbie/internal/tiny"
)

// collectSums reads every instrument the reader holds and returns the single data point of
// each sum instrument, keyed by instrument name.
func collectSums(t *testing.T, reader sdkmetric.Reader) map[string]float64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	sums := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					sums[m.Name] = float64(dp.Value)
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					sums[m.Name] = dp.Value
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					sums[m.Name] = float64(dp.Value)
				}
			}
		}
	}

	return sums
}

// TestTelemetryRecordsEventLoopOccupancy checks that the time the coordinator's event loop
// spends working is reported through the supplied meter provider, so that its occupancy can
// be derived from the rate at which that time accumulates.
func TestTelemetryRecordsEventLoopOccupancy(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tele, err := NewTelemetry(provider, noop.NewTracerProvider())
	require.NoError(t, err)

	tele.RecordEventLoopPass(ctx, 50*time.Millisecond)
	tele.RecordEventLoopPass(ctx, 150*time.Millisecond)

	sums := collectSums(t, reader)

	require.InDelta(t, 0.2, sums["coordinator_event_loop_busy_seconds"], 1e-9)
	require.Equal(t, float64(2), sums["coordinator_event_loop_passes"])
}

// TestCoordinatorReportsThroughInjectedProvider checks that the instruments held by the
// coordinator and by each of its behaviours reach the meter provider it was configured with,
// rather than the global one the default configuration starts from.
func TestCoordinatorReportsThroughInjectedProvider(t *testing.T) {
	ctx := kadtest.CtxShort(t)

	_, nodes, err := linearTopology(4)
	require.NoError(t, err)

	reader := sdkmetric.NewManualReader()

	ccfg := DefaultCoordinatorConfig[tiny.Key, tiny.Node, tiny.Message]()
	ccfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	c, err := NewCoordinator[tiny.Key, tiny.Node, tiny.Message](nodes[0].NodeID, nodes[0].Router, nodes[0].RoutingTable, tiny.NodeWithCpl, ccfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	qfn := func(ctx context.Context, id tiny.Node, msg tiny.Message, stats coordt.QueryStats) error {
		return nil
	}

	_, _, err = c.QueryClosest(ctx, nodes[3].NodeID.Key(), qfn, 20)
	require.NoError(t, err)

	sums := collectSums(t, reader)

	require.Positive(t, sums["coordinator_event_loop_passes"], "event loop reported no passes")

	require.Contains(t, sums, "coordinator_event_loop_busy_seconds", "event loop busy time was not reported")

	// the behaviour instruments are registered even when nothing has been dropped, so their
	// presence is what shows the injected provider reached them
	for _, name := range []string{
		"query_inbound_queue_depth",
		"routing_inbound_queue_depth",
		"broadcast_inbound_queue_depth",
		"network_requests_in_flight",
		"network_node_handlers",
	} {
		require.Contains(t, sums, name)
	}
}
