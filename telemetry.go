package xorbie

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// meterName and tracerName name this package as the source of the telemetry it emits.
const (
	meterName  = "github.com/iand/xorbie"
	tracerName = "github.com/iand/xorbie"
)

// Telemetry is the struct that holds a reference to all metrics and the tracer used
// by the coordinator and its components.
// Make sure to also register the [MeterProviderOpts] with your custom or the global
// [metric.MeterProvider].
type Telemetry struct {
	Tracer trace.Tracer

	// eventLoopBusySeconds accumulates the time the event loop has spent performing work.
	//
	// This is a lower bound since event loops may complete faster than the platform's
	// clock resolution.
	eventLoopBusySeconds metric.Float64Counter

	// eventLoopPasses counts the passes the event loop has made.
	eventLoopPasses metric.Int64Counter
}

// NewTelemetry initializes a Telemetry struct with the given meter and tracer providers.
func NewTelemetry(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*Telemetry, error) {
	meter := meterProvider.Meter(meterName)

	t := &Telemetry{
		Tracer: tracerProvider.Tracer(tracerName),
	}

	var err error

	t.eventLoopBusySeconds, err = meter.Float64Counter(
		"coordinator_event_loop_busy_seconds",
		metric.WithDescription("Total time the coordinator's event loop has spent performing work"),
	)
	if err != nil {
		return nil, fmt.Errorf("create coordinator_event_loop_busy_seconds counter: %w", err)
	}

	t.eventLoopPasses, err = meter.Int64Counter(
		"coordinator_event_loop_passes",
		metric.WithDescription("Total number of passes the coordinator's event loop has made"),
	)
	if err != nil {
		return nil, fmt.Errorf("create coordinator_event_loop_passes counter: %w", err)
	}

	return t, nil
}

// logAttrNodeID returns a log attribute naming a node.
func logAttrNodeID(n fmt.Stringer) slog.Attr {
	return slog.String("node_id", n.String())
}

// logAttrKey returns a log attribute naming a Kademlia key. A key is not required to carry a
// string form, so the value is left for the handler to format.
func logAttrKey(k any) slog.Attr {
	return slog.Any("key", k)
}

// RecordEventLoopPass records one pass of the coordinator's event loop and the time it spent
// working. The rate at which that time accumulates is the loop's occupancy: the fraction of
// wall clock time its single worker goroutine is unavailable to take on anything else.
func (t *Telemetry) RecordEventLoopPass(ctx context.Context, busy time.Duration) {
	t.eventLoopBusySeconds.Add(ctx, busy.Seconds())
	t.eventLoopPasses.Add(ctx, 1)
}

// logAttrError returns a log attribute carrying an error.
func logAttrError(err error) slog.Attr {
	return slog.Any("error", err)
}
