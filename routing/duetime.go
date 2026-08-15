package routing

import "time"

// earlier returns the earlier of two due times, treating the zero time as "no time
// scheduled" rather than as the earliest possible instant. It returns the zero time
// only when both arguments are zero.
func earlier(a, b time.Time) time.Time {
	switch {
	case a.IsZero():
		return b
	case b.IsZero():
		return a
	case b.Before(a):
		return b
	default:
		return a
	}
}
