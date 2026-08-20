package publish

import (
	"testing"

	"github.com/iand/xorbie/internal/tiny"
)

func TestPublishStateInterfaceConformance(t *testing.T) {
	states := []PublishState{
		&StatePublishIdle{},
		&StatePublishWaiting{},
		&StatePublishStoreRecord[tiny.Key, tiny.Node, tiny.Message]{},
		&StatePublishFindCloser[tiny.Key, tiny.Node]{},
		&StatePublishFinished[tiny.Key, tiny.Node]{},
	}
	for _, st := range states {
		st.publishState() // drives test coverage
	}
}

func TestPublishEventInterfaceConformance(t *testing.T) {
	events := []PublishEvent{
		&EventPublishStop{},
		&EventPublishPoll{},
		&EventPublishStart[tiny.Key, tiny.Node]{},
		&EventPublishNodeResponse[tiny.Key, tiny.Node]{},
		&EventPublishNodeFailure[tiny.Key, tiny.Node]{},
		&EventPublishStoreRecordSuccess[tiny.Key, tiny.Node, tiny.Message]{},
		&EventPublishStoreRecordFailure[tiny.Key, tiny.Node, tiny.Message]{},
	}
	for _, ev := range events {
		ev.publishEvent() // drives test coverage
	}
}
