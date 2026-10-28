package sharedwork

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoLeaderCancellationKeepsWorkForFollower(t *testing.T) {
	group := New[string](time.Second)
	demand := Demand{AuthorizationScope: "auth", SemanticKey: "report", ForceAudit: true}
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	started := make(chan struct{})
	workCanceled := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := group.Do(leaderCtx, "org\x00proj", demand, func(workCtx context.Context) (string, error) {
			runs.Add(1)
			close(started)
			go func() {
				<-workCtx.Done()
				close(workCanceled)
			}()
			<-release
			return "complete", nil
		})
		leaderDone <- err
	}()
	<-started

	followerObserved := make(chan struct{})
	followerCtx, cancelFollower := newObservedContext(followerObserved, 500*time.Millisecond)
	defer cancelFollower()
	followerDone := make(chan struct {
		value string
		err   error
	}, 1)
	go func() {
		value, _, err := group.Do(followerCtx, "org\x00proj", demand, func(context.Context) (string, error) {
			runs.Add(1)
			return "duplicate", nil
		})
		followerDone <- struct {
			value string
			err   error
		}{value, err}
	}()
	<-followerObserved
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context cancellation", err)
	}
	select {
	case <-workCanceled:
		t.Fatal("shared work stopped while the follower was still waiting")
	default:
	}
	close(release)
	result := <-followerDone
	<-workCanceled
	if result.err != nil || result.value != "complete" || runs.Load() != 1 {
		t.Fatalf("follower result=%q error=%v runs=%d", result.value, result.err, runs.Load())
	}
}

func TestDoLastWaiterCancellationRetainsEntryUntilWorkerExits(t *testing.T) {
	group := New[string](time.Second)
	demand := Demand{AuthorizationScope: "auth", SemanticKey: "report"}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	started := make(chan struct{})
	workCanceled := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := group.Do(firstCtx, "org\x00proj", demand, func(workCtx context.Context) (string, error) {
			runs.Add(1)
			close(started)
			<-workCtx.Done()
			close(workCanceled)
			<-release
			return "old", workCtx.Err()
		})
		firstDone <- err
	}()
	<-started
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller error = %v, want context cancellation", err)
	}
	<-workCanceled

	newCallerObserved := make(chan struct{})
	newCallerCtx, cancelNewCaller := newObservedContext(newCallerObserved, 500*time.Millisecond)
	defer cancelNewCaller()
	newDone := make(chan struct {
		value string
		err   error
	}, 1)
	go func() {
		value, _, err := group.Do(newCallerCtx, "org\x00proj", demand, func(context.Context) (string, error) {
			runs.Add(1)
			return "new", nil
		})
		newDone <- struct {
			value string
			err   error
		}{value, err}
	}()
	<-newCallerObserved
	if got := runs.Load(); got != 1 {
		t.Fatalf("new work started before abandoned worker exited, runs=%d", got)
	}
	close(release)
	result := <-newDone
	if result.err != nil || result.value != "new" || runs.Load() != 2 {
		t.Fatalf("new caller result=%q error=%v runs=%d", result.value, result.err, runs.Load())
	}
}

func TestDoQueueWaitUsesCallerBudgetWithoutCancelingActiveWork(t *testing.T) {
	group := New[string](40 * time.Millisecond)
	demand := Demand{AuthorizationScope: "auth", SemanticKey: "first"}
	started := make(chan struct{})
	release := make(chan struct{})
	drained := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := group.Do(context.Background(), "org\x00proj", demand, func(context.Context) (string, error) {
			close(started)
			<-release
			close(drained)
			return "first", nil
		})
		firstDone <- err
	}()
	<-started
	if err := <-firstDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first waiter error = %v, want work budget expiration", err)
	}

	var secondRuns atomic.Int32
	_, _, err := group.Do(context.Background(), "org\x00proj", Demand{AuthorizationScope: "auth", SemanticKey: "second"}, func(context.Context) (string, error) {
		secondRuns.Add(1)
		return "second", nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || secondRuns.Load() != 0 {
		t.Fatalf("queued caller error=%v callback_runs=%d", err, secondRuns.Load())
	}
	close(release)
	<-drained
}

func TestDoStrongerDemandWaitsThenRuns(t *testing.T) {
	group := New[string](time.Second)
	weak := Demand{AuthorizationScope: "auth", SemanticKey: "report", ForceAudit: true}
	strong := Demand{AuthorizationScope: "auth", SemanticKey: "report", ForceAudit: true, ForceBuckets: true}
	started := make(chan struct{})
	release := make(chan struct{})
	weakDone := make(chan error, 1)
	go func() {
		_, _, err := group.Do(context.Background(), "org\x00proj", weak, func(context.Context) (string, error) {
			close(started)
			<-release
			return "weak", nil
		})
		weakDone <- err
	}()
	<-started

	strongObserved := make(chan struct{})
	strongCtx, cancelStrong := newObservedContext(strongObserved, 500*time.Millisecond)
	defer cancelStrong()
	strongDone := make(chan struct {
		value  string
		joined bool
		err    error
	}, 1)
	var strongRuns atomic.Int32
	go func() {
		value, joined, err := group.Do(strongCtx, "org\x00proj", strong, func(context.Context) (string, error) {
			strongRuns.Add(1)
			return "strong", nil
		})
		strongDone <- struct {
			value  string
			joined bool
			err    error
		}{value, joined, err}
	}()
	<-strongObserved
	close(release)
	if err := <-weakDone; err != nil {
		t.Fatalf("weaker work error = %v", err)
	}
	result := <-strongDone
	if result.err != nil || result.joined || result.value != "strong" || strongRuns.Load() != 1 {
		t.Fatalf("strong result=%q joined=%t error=%v runs=%d", result.value, result.joined, result.err, strongRuns.Load())
	}
}

func TestDoRejectsDifferentAuthOrSemanticIdentity(t *testing.T) {
	active := Demand{AuthorizationScope: "auth-a", SemanticKey: "report-a"}
	otherAuth := active
	otherAuth.AuthorizationScope = "auth-b"
	otherRequest := active
	otherRequest.SemanticKey = "report-b"
	if active.satisfies(otherAuth) || active.satisfies(otherRequest) {
		t.Fatal("incompatible authorization or request identity joined active work")
	}
}

func TestDoDoesNotShareDifferentAuthOrRequestIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		active    Demand
		request   Demand
		activeID  string
		requestID string
	}{
		{
			name:      "authorization scope",
			active:    Demand{AuthorizationScope: "auth-a", SemanticKey: "report"},
			request:   Demand{AuthorizationScope: "auth-b", SemanticKey: "report"},
			activeID:  "scope-a",
			requestID: "scope-b",
		},
		{
			name:      "semantic request",
			active:    Demand{AuthorizationScope: "auth", SemanticKey: "report-a"},
			request:   Demand{AuthorizationScope: "auth", SemanticKey: "report-b"},
			activeID:  "report-a",
			requestID: "report-b",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			group := New[string](time.Second)
			started := make(chan struct{})
			release := make(chan struct{})
			activeDone := make(chan string, 1)
			go func() {
				value, _, _ := group.Do(context.Background(), "org\x00proj", test.active, func(context.Context) (string, error) {
					close(started)
					<-release
					return test.activeID, nil
				})
				activeDone <- value
			}()
			<-started

			requestObserved := make(chan struct{})
			requestCtx, cancelRequest := newObservedContext(requestObserved, 500*time.Millisecond)
			defer cancelRequest()
			requestDone := make(chan struct {
				value  string
				joined bool
				err    error
			}, 1)
			go func() {
				value, joined, err := group.Do(requestCtx, "org\x00proj", test.request, func(context.Context) (string, error) {
					return test.requestID, nil
				})
				requestDone <- struct {
					value  string
					joined bool
					err    error
				}{value, joined, err}
			}()
			<-requestObserved
			close(release)
			if got := <-activeDone; got != test.activeID {
				t.Fatalf("active result = %q, want %q", got, test.activeID)
			}
			result := <-requestDone
			if result.err != nil || result.joined || result.value != test.requestID {
				t.Fatalf("request result=%q joined=%t error=%v", result.value, result.joined, result.err)
			}
		})
	}
}

func TestDoAllowsDifferentProjectsToRunConcurrently(t *testing.T) {
	group := New[string](time.Second)
	started := make(chan string, 2)
	release := make(chan struct{})
	results := make(chan string, 2)
	for _, project := range []string{"org\x00project-a", "org\x00project-b"} {
		project := project
		go func() {
			value, _, err := group.Do(context.Background(), project, Demand{}, func(context.Context) (string, error) {
				started <- project
				<-release
				return project, nil
			})
			if err != nil {
				results <- err.Error()
				return
			}
			results <- value
		}()
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case project := <-started:
			seen[project] = true
		case <-time.After(time.Second):
			t.Fatal("different project work did not start concurrently")
		}
	}
	close(release)
	for range 2 {
		if got := <-results; got == "" || !seen[got] {
			t.Fatalf("unexpected project result %q", got)
		}
	}
}

func TestDoPreCanceledCallerDoesNotStartWork(t *testing.T) {
	group := New[string](time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var runs atomic.Int32
	_, _, err := group.Do(ctx, "org\x00proj", Demand{}, func(context.Context) (string, error) {
		runs.Add(1)
		return "unexpected", nil
	})
	if !errors.Is(err, context.Canceled) || runs.Load() != 0 {
		t.Fatalf("error=%v runs=%d", err, runs.Load())
	}
}

func TestShutdownCancelsWorkAndRejectsNewRequests(t *testing.T) {
	group := New[string](time.Second)
	started := make(chan struct{})
	finished := make(chan struct{})
	workDone := make(chan error, 1)
	go func() {
		_, _, err := group.Do(context.Background(), "org\x00proj", Demand{}, func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			close(finished)
			return "", ctx.Err()
		})
		workDone <- err
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
	<-finished
	if err := <-workDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("work error = %v, want cancellation", err)
	}
	if _, _, err := group.Do(context.Background(), "org\x00proj", Demand{}, func(context.Context) (string, error) {
		return "unexpected", nil
	}); !errors.Is(err, ErrShutdown) {
		t.Fatalf("new call error = %v, want ErrShutdown", err)
	}
}

func TestPanicFinishesWorkAndReleasesProjectSlot(t *testing.T) {
	group := New[string](time.Second)
	if _, _, err := group.Do(context.Background(), "org\x00proj", Demand{}, func(context.Context) (string, error) {
		panic("unexpected")
	}); !errors.Is(err, ErrPanic) {
		t.Fatalf("panic result error = %v, want ErrPanic", err)
	}
	value, joined, err := group.Do(context.Background(), "org\x00proj", Demand{}, func(context.Context) (string, error) {
		return "recovered", nil
	})
	if err != nil || joined || value != "recovered" {
		t.Fatalf("call after panic value=%q joined=%t error=%v", value, joined, err)
	}
}

type observedContext struct {
	context.Context
	observed chan struct{}
	deadline time.Time
}

func (ctx observedContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func newObservedContext(observed chan struct{}, timeout time.Duration) (observedContext, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	deadline, _ := ctx.Deadline()
	return observedContext{Context: ctx, observed: observed, deadline: deadline}, cancel
}

func (ctx observedContext) Done() <-chan struct{} {
	select {
	case <-ctx.observed:
	default:
		close(ctx.observed)
	}
	return ctx.Context.Done()
}
