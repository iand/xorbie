package brdcst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptimisticConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultOptimisticConfig()
		require.NoError(t, cfg.Validate())
	})
}
