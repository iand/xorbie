package brdcst

// The Optimistic state machine is not implemented. Starting a broadcast with an
// [OptimisticConfig] panics.

// OptimisticConfig specifies the configuration for the Optimistic state machine.
type OptimisticConfig struct{}

func (c *OptimisticConfig) broadcastConfig() {}

// Validate checks the configuration options and returns an error if any have
// invalid values.
func (c *OptimisticConfig) Validate() error {
	return nil
}

// DefaultOptimisticConfig returns the default configuration options for the
// Optimistic state machine.
func DefaultOptimisticConfig() *OptimisticConfig {
	return &OptimisticConfig{}
}
