package brdcst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("query pool config is not nil", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.pCfg = nil
		require.Error(t, cfg.Validate())
	})

	t.Run("tracer is not nil", func(t *testing.T) {
		cfg := DefaultPoolConfig()
		cfg.Tracer = nil
		require.Error(t, cfg.Validate())
	})
}

func TestFollowUpConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultFollowUpConfig()
		require.NoError(t, cfg.Validate())
	})
}

func TestOptimisticConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultOptimisticConfig()
		require.NoError(t, cfg.Validate())
	})
}

func TestStaticConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultStaticConfig()
		require.NoError(t, cfg.Validate())
	})
}

func TestConfigInterfaceConformance(t *testing.T) {
	configs := []Config{
		&FollowUpConfig{},
		&OptimisticConfig{},
		&StaticConfig{},
	}
	for _, c := range configs {
		c.broadcastConfig() // drives test coverage
	}
}
