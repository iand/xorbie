package brdcst

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/query"
)

// PoolConfig specifies the configuration for a broadcast [Pool].
type PoolConfig struct {
	pCfg *query.PoolConfig

	// Tracer is the tracer that should be used to trace execution.
	Tracer trace.Tracer
}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (cfg *PoolConfig) Validate() error {
	if cfg.pCfg == nil {
		return fmt.Errorf("query pool config must not be nil")
	}

	if cfg.Tracer == nil {
		return fmt.Errorf("tracer must not be nil")
	}

	return nil
}

// DefaultPoolConfig returns the default configuration options for a Pool.
// Options may be overridden before passing to NewPool
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		pCfg:   query.DefaultPoolConfig(),
		Tracer: coordt.NoopTracer(),
	}
}

// Config is an interface that all broadcast configurations must implement. A record can be
// broadcast in more than one way, and the concrete type of the [Config] carried by an
// [EventPoolStartBroadcast] event selects which strategy the [Pool] creates to run it.
type Config interface {
	broadcastConfig()
}

func (c *FollowUpConfig) broadcastConfig()   {}
func (c *OptimisticConfig) broadcastConfig() {}
func (c *StaticConfig) broadcastConfig()     {}

// FollowUpConfig specifies the configuration for the [FollowUp] state machine.
type FollowUpConfig struct{}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *FollowUpConfig) Validate() error {
	return nil
}

// DefaultFollowUpConfig returns the default configuration options for the
// [FollowUp] state machine.
func DefaultFollowUpConfig() *FollowUpConfig {
	return &FollowUpConfig{}
}

// OptimisticConfig specifies the configuration for the [Optimistic] state
// machine.
type OptimisticConfig struct{}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *OptimisticConfig) Validate() error {
	return nil
}

// DefaultOptimisticConfig returns the default configuration options for the
// [Optimistic] state machine.
func DefaultOptimisticConfig() *OptimisticConfig {
	return &OptimisticConfig{}
}

// StaticConfig specifies the configuration for the [Static] state
// machine.
type StaticConfig struct{}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *StaticConfig) Validate() error {
	return nil
}

// DefaultStaticConfig returns the default configuration options for the
// [Static] state machine.
func DefaultStaticConfig() *StaticConfig {
	return &StaticConfig{}
}
