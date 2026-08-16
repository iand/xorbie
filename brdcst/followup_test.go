package brdcst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFollowUpConfigValidate(t *testing.T) {
	t.Run("default is valid", func(t *testing.T) {
		cfg := DefaultFollowUpConfig()
		require.NoError(t, cfg.Validate())
	})
}
