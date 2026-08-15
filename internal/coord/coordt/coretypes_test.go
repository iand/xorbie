package coordt_test

import (
	"github.com/iand/xorbie/internal/coord/coordt"
	"github.com/iand/xorbie/internal/tiny"
)

var (
	_ coordt.Message[tiny.Key, tiny.Node] = tiny.Message{}
	_ coordt.Message[tiny.Key, tiny.Node] = coordt.NoMessage[tiny.Key, tiny.Node]{}
)
