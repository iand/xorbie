package tiny_test

import (
	"github.com/iand/xorbie/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

var _ coordt.Message[tiny.Key, tiny.Node] = tiny.Message{}
