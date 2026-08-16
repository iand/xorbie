package brdcst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultStaticConfig()
		require.NoError(t, cfg.Validate())
	})
}
