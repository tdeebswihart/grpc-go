package quota

import (
	"context"
	"sync/atomic"
)

// Ticket is a sentinel type used to represent a request ticket.
// Its implementation is enforcer-dependent and opaque to the rest
// of the system.
type Ticket interface{}

// Enforcer implementations allow for the enforcement of request quotas.
type Enforcer interface {
	// Acquire a request token. This blocks until a token is available,
	// and returns whether one was granted. The only time a token is not
	// granted is when the provided context expires before one is available.
	Acquire(context.Context) Ticket
	// Release a request token.
	Release(Ticket)
}

type emptyTicket struct{}

// StaticQuotaEnforcer implements a blocking, counting semaphore that enforces
// a static maximum number of concurrent requests. Acquire should be
// called synchronously; Release may be called asynchronously.
type StaticQuotaEnforcer struct {
	n    atomic.Int64
	wait chan struct{}
}

func (q *StaticQuotaEnforcer) Acquire(ctx context.Context) Ticket {
	if q.n.Add(-1) < 0 {
		// We ran out of quota. Block until a release happens or our
		// context is canceled.
		select {
		case <-ctx.Done():
			return false
		case <-q.wait:
		}
	}
	return emptyTicket{}
}

func (q *StaticQuotaEnforcer) Release(_ Ticket) {
	// N.B. the "<= 0" check below should allow for this to work with multiple
	// concurrent calls to acquire, but also note that with synchronous calls to
	// acquire, as our system does, n will never be less than -1.  There are
	// fairness issues (queuing) to consider if this was to be generalized.
	if q.n.Add(1) <= 0 {
		// An acquire was waiting on us.  Unblock it.
		q.wait <- struct{}{}
	}
}

func NewStaticQuotaEnforcer(n uint32) *StaticQuotaEnforcer {
	a := &StaticQuotaEnforcer{wait: make(chan struct{}, 1)}
	a.n.Store(int64(n))
	return a
}
