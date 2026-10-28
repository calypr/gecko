package sharedwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"
)

const DefaultBudget = 15 * time.Minute

var ErrShutdown = errors.New("shared audit work is shutting down")
var ErrPanic = errors.New("shared audit work panicked")

type Demand struct {
	AuthorizationScope string
	SemanticKey        string
	ForceAudit         bool
	ForceBuckets       bool
	ForceStorage       bool
}

func (demand Demand) satisfies(request Demand) bool {
	if demand.AuthorizationScope != request.AuthorizationScope || demand.SemanticKey != request.SemanticKey {
		return false
	}
	return (!request.ForceAudit || demand.ForceAudit) &&
		(!request.ForceBuckets || demand.ForceBuckets || demand.ForceStorage) &&
		(!request.ForceStorage || demand.ForceStorage)
}

func AuthorizationScope(authorizationHeader string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(authorizationHeader)))
	return hex.EncodeToString(sum[:])
}

type state uint8

const (
	running state = iota
	abandoned
	finished
)

type work[T any] struct {
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	demand  Demand
	waiters int
	state   state
	value   T
	err     error
}

type Group[T any] struct {
	mu     sync.Mutex
	budget time.Duration
	closed bool
	work   map[string]*work[T]
}

func New[T any](budget time.Duration) *Group[T] {
	if budget <= 0 {
		budget = DefaultBudget
	}
	return &Group[T]{budget: budget, work: map[string]*work[T]{}}
}

func (group *Group[T]) Do(callerCtx context.Context, projectKey string, demand Demand, run func(context.Context) (T, error)) (T, bool, error) {
	var zero T
	waitCtx := callerCtx
	cancelWait := func() {}
	if deadline, ok := callerCtx.Deadline(); !ok || time.Until(deadline) > group.budget {
		waitCtx, cancelWait = context.WithTimeout(callerCtx, group.budget)
	}
	defer cancelWait()
	for {
		if err := waitCtx.Err(); err != nil {
			return zero, false, err
		}
		group.mu.Lock()
		if err := waitCtx.Err(); err != nil {
			group.mu.Unlock()
			return zero, false, err
		}
		if group.closed {
			group.mu.Unlock()
			return zero, false, ErrShutdown
		}
		current := group.work[projectKey]
		if current != nil {
			if current.state == running && current.demand.satisfies(demand) {
				current.waiters++
				group.mu.Unlock()
				return await[T](group, waitCtx, current, true)
			}
			done := current.done
			group.mu.Unlock()
			select {
			case <-waitCtx.Done():
				return zero, false, waitCtx.Err()
			case <-done:
				continue
			}
		}
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(callerCtx), group.budget)
		current = &work[T]{
			done:    make(chan struct{}),
			ctx:     workCtx,
			cancel:  cancel,
			demand:  demand,
			waiters: 1,
			state:   running,
		}
		group.work[projectKey] = current
		group.mu.Unlock()
		go group.execute(projectKey, current, run)
		return await[T](group, waitCtx, current, false)
	}
}

func await[T any](group *Group[T], callerCtx context.Context, current *work[T], joined bool) (T, bool, error) {
	var zero T
	select {
	case <-callerCtx.Done():
		group.leave(current)
		return zero, joined, callerCtx.Err()
	case <-current.done:
		if err := callerCtx.Err(); err != nil {
			group.leave(current)
			return zero, joined, err
		}
		group.leave(current)
		return current.value, joined, current.err
	case <-current.ctx.Done():
		select {
		case <-current.done:
			if err := callerCtx.Err(); err != nil {
				group.leave(current)
				return zero, joined, err
			}
			group.leave(current)
			return current.value, joined, current.err
		default:
			group.leave(current)
			return zero, joined, current.ctx.Err()
		}
	}
}

func (group *Group[T]) execute(projectKey string, current *work[T], run func(context.Context) (T, error)) {
	var value T
	var err error
	func() {
		defer func() {
			if recover() != nil {
				err = ErrPanic
			}
		}()
		value, err = run(current.ctx)
	}()
	group.mu.Lock()
	current.value = value
	current.err = err
	current.state = finished
	if group.work[projectKey] == current {
		delete(group.work, projectKey)
	}
	close(current.done)
	group.mu.Unlock()
	current.cancel()
}

func (group *Group[T]) leave(current *work[T]) {
	var cancel context.CancelFunc
	group.mu.Lock()
	if current.waiters > 0 {
		current.waiters--
	}
	if current.state == running && current.waiters == 0 {
		current.state = abandoned
		cancel = current.cancel
	}
	group.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (group *Group[T]) Shutdown(ctx context.Context) error {
	group.mu.Lock()
	group.closed = true
	done := make([]<-chan struct{}, 0, len(group.work))
	cancels := make([]context.CancelFunc, 0, len(group.work))
	for _, current := range group.work {
		done = append(done, current.done)
		cancels = append(cancels, current.cancel)
	}
	group.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	for _, completed := range done {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-completed:
		}
	}
	return nil
}
