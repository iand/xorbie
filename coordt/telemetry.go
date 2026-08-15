package coordt

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Attribute keys used when tracing the execution of a state machine.
const (
	AttrKeyInEvent  = "in_event"
	AttrKeyOutEvent = "out_event"
)

// AttrInEvent creates an attribute that records the type of an event being supplied to a state machine.
func AttrInEvent(t any) attribute.KeyValue {
	return attribute.String(AttrKeyInEvent, fmt.Sprintf("%T", t))
}

// AttrOutEvent creates an attribute that records the type of an event being returned by a state machine.
func AttrOutEvent(t any) attribute.KeyValue {
	return attribute.String(AttrKeyOutEvent, fmt.Sprintf("%T", t))
}

// NoopTracer returns a tracer that does not emit traces. It is the default for any
// component that has not been given a tracer.
func NoopTracer() trace.Tracer {
	return tracenoop.NewTracerProvider().Tracer("")
}

// NoopMeter returns a meter that does not record or emit metrics. It is the default
// for any component that has not been given a meter.
func NoopMeter() metric.Meter {
	return noop.NewMeterProvider().Meter("")
}
