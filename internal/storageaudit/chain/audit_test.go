package chain

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/storecache"
)

func TestBucketRefreshDoesNotJoinWeakerAuditRefresh(t *testing.T) {
	service := New(Dependencies{})
	weakStarted := make(chan struct{})
	releaseWeak := make(chan struct{})
	weakDone := make(chan error, 1)
	weak := storecache.CachedResponse{CachedAt: time.Now(), Response: model.ChainAuditResponse{Summary: model.ChainSummary{GitRevision: "weak"}}}
	go func() {
		_, _, err := service.coalesceStorageChainAuditRefresh(context.Background(), "org", "proj", "Bearer token", "key", true, false, false, func(context.Context) (StorageChainAuditRefreshResult, error) {
			close(weakStarted)
			<-releaseWeak
			return StorageChainAuditRefreshResult{Response: weak}, nil
		})
		weakDone <- err
	}()
	<-weakStarted

	observedWait := make(chan struct{})
	strongCtx := &waitObservedContext{Context: context.Background(), observed: observedWait}
	strongDone := make(chan struct {
		value  StorageChainAuditRefreshResult
		joined bool
		err    error
	}, 1)
	var bucketRefreshCalls int
	strong := storecache.CachedResponse{CachedAt: time.Now(), Response: model.ChainAuditResponse{Summary: model.ChainSummary{GitRevision: "bucket"}}}
	go func() {
		value, joined, err := service.coalesceStorageChainAuditRefresh(strongCtx, "org", "proj", "Bearer token", "key", true, true, false, func(context.Context) (StorageChainAuditRefreshResult, error) {
			bucketRefreshCalls++
			return StorageChainAuditRefreshResult{Response: strong}, nil
		})
		strongDone <- struct {
			value  StorageChainAuditRefreshResult
			joined bool
			err    error
		}{value, joined, err}
	}()
	<-observedWait
	close(releaseWeak)
	if err := <-weakDone; err != nil {
		t.Fatalf("weaker audit refresh failed: %v", err)
	}
	result := <-strongDone
	if result.err != nil || result.joined || result.value.Response.Response.Summary.GitRevision != "bucket" || bucketRefreshCalls != 1 {
		t.Fatalf("bucket refresh joined weaker work: result=%+v calls=%d", result, bucketRefreshCalls)
	}
}

type waitObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (ctx *waitObservedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}
