package storecache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	projectAuditLeaseKeyPrefix = "gecko:storage_chain_audit:project_lease:v1:"
	projectAuditLeaseDuration  = 30 * time.Second
	projectAuditRenewInterval  = 10 * time.Second
	projectAuditWaitInterval   = 100 * time.Millisecond
	projectAuditRedisTimeout   = 5 * time.Second
)

var ErrProjectAuditLeaseLost = errors.New("storage audit project lease lost")

// ProjectAuditCoordinator provides Redis-backed admission for one organization and project.
type ProjectAuditCoordinator interface {
	AcquireProject(ctx context.Context, organization string, project string) (ProjectAuditLease, error)
}

// ProjectAuditLease owns one project refresh until release or lease loss.
type ProjectAuditLease interface {
	Context() context.Context
	PublishResponse(ctx context.Context, key string, value CachedResponse) error
	PublishBucketInventory(ctx context.Context, key string, value CachedBucketInventory, ttl time.Duration) error
	Release(ctx context.Context) error
}

// RedisProjectAuditCoordinator coordinates project refreshes through Redis.
type RedisProjectAuditCoordinator struct {
	client        *redis.Client
	leaseDuration time.Duration
	renewInterval time.Duration
}

type redisProjectAuditLease struct {
	client        *redis.Client
	key           string
	owner         string
	leaseDuration time.Duration
	renewInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	renewCtx      context.Context
	renewCancel   context.CancelFunc
	renewDone     chan struct{}
	releaseOnce   sync.Once
	releaseDone   chan struct{}
	releaseErr    error
	lossMu        sync.Mutex
	lossErr       error
}

const projectAuditRenewLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
`

const projectAuditReleaseLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
	return 0
end
return redis.call('DEL', KEYS[1])
`

const projectAuditPublishLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
	return 0
end
if ARGV[3] == '0' then
	redis.call('SET', KEYS[2], ARGV[2])
else
	redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
end
return 1
`

// NewRedisProjectAuditCoordinator connects to the required Redis instance and verifies it is reachable.
func NewRedisProjectAuditCoordinator(redisURL string) (*RedisProjectAuditCoordinator, error) {
	client, err := newRedisClient(redisURL)
	if err != nil {
		return nil, err
	}
	return newRedisProjectAuditCoordinator(client, projectAuditLeaseDuration, projectAuditRenewInterval), nil
}

// NewRedisProjectAuditCoordinatorFromEnv uses the required storage audit Redis configuration.
func NewRedisProjectAuditCoordinatorFromEnv() (*RedisProjectAuditCoordinator, error) {
	redisURL, err := requiredStorageAuditRedisURLFromEnv()
	if err != nil {
		return nil, err
	}
	coordinator, err := NewRedisProjectAuditCoordinator(redisURL)
	if err != nil {
		return nil, fmt.Errorf("initialize storage audit project coordinator: %w", err)
	}
	return coordinator, nil
}

func newRedisProjectAuditCoordinator(client *redis.Client, leaseDuration time.Duration, renewInterval time.Duration) *RedisProjectAuditCoordinator {
	return &RedisProjectAuditCoordinator{
		client:        client,
		leaseDuration: leaseDuration,
		renewInterval: renewInterval,
	}
}

func (coordinator *RedisProjectAuditCoordinator) Close() error {
	return coordinator.client.Close()
}

func (coordinator *RedisProjectAuditCoordinator) AcquireProject(ctx context.Context, organization string, project string) (ProjectAuditLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := projectAuditLeaseKey(organization, project)
	if err != nil {
		return nil, err
	}
	owner, err := newProjectAuditOwnerToken()
	if err != nil {
		return nil, fmt.Errorf("create storage audit project lease owner: %w", err)
	}

	for {
		acquired, err := coordinator.client.SetNX(ctx, key, owner, coordinator.leaseDuration).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("acquire storage audit project lease: %w", err)
		}
		if acquired {
			if err := ctx.Err(); err != nil {
				coordinator.releaseOwnerAfterCanceledAcquire(key, owner)
				return nil, err
			}
			leaseCtx, cancel := context.WithCancel(ctx)
			renewCtx, renewCancel := context.WithCancel(context.Background())
			lease := &redisProjectAuditLease{
				client:        coordinator.client,
				key:           key,
				owner:         owner,
				leaseDuration: coordinator.leaseDuration,
				renewInterval: coordinator.renewInterval,
				ctx:           leaseCtx,
				cancel:        cancel,
				renewCtx:      renewCtx,
				renewCancel:   renewCancel,
				renewDone:     make(chan struct{}),
				releaseDone:   make(chan struct{}),
			}
			go lease.renew()
			return lease, nil
		}

		timer := time.NewTimer(projectAuditWaitInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (coordinator *RedisProjectAuditCoordinator) releaseOwnerAfterCanceledAcquire(key string, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), projectAuditRedisTimeout)
	defer cancel()
	_ = coordinator.client.Eval(ctx, projectAuditReleaseLua, []string{key}, owner).Err()
}

func projectAuditLeaseKey(organization string, project string) (string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || project == "" {
		return "", errors.New("storage audit project lease requires organization and project")
	}
	var identity strings.Builder
	identity.WriteString(strconv.Itoa(len(organization)))
	identity.WriteByte(':')
	identity.WriteString(organization)
	identity.WriteString(strconv.Itoa(len(project)))
	identity.WriteByte(':')
	identity.WriteString(project)
	sum := sha256.Sum256([]byte(identity.String()))
	return projectAuditLeaseKeyPrefix + hex.EncodeToString(sum[:]), nil
}

func newProjectAuditOwnerToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (lease *redisProjectAuditLease) Context() context.Context {
	return lease.ctx
}

func (lease *redisProjectAuditLease) renew() {
	defer close(lease.renewDone)
	ticker := time.NewTicker(lease.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-lease.renewCtx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(lease.renewCtx, projectAuditRedisTimeout)
			result, err := lease.client.Eval(ctx, projectAuditRenewLua, []string{lease.key}, lease.owner, lease.leaseDuration.Milliseconds()).Int()
			cancel()
			if err != nil {
				lease.lose(fmt.Errorf("renew storage audit project lease: %w", err))
				return
			}
			if result != 1 {
				lease.lose(ErrProjectAuditLeaseLost)
				return
			}
		}
	}
}

func (lease *redisProjectAuditLease) PublishResponse(ctx context.Context, key string, value CachedResponse) error {
	if err := lease.validatePublication(ctx, key); err != nil {
		return err
	}
	raw, err := encodeCachedResponse(ctx, value)
	if err != nil {
		if ctx.Err() != nil {
			if validationErr := lease.validatePublication(ctx, key); validationErr != nil {
				return validationErr
			}
		}
		return err
	}
	return lease.publishRaw(ctx, key, raw, 0)
}

func (lease *redisProjectAuditLease) PublishBucketInventory(ctx context.Context, key string, value CachedBucketInventory, ttl time.Duration) error {
	if ttl < 0 {
		return errors.New("storage audit bucket inventory TTL cannot be negative")
	}
	return lease.publish(ctx, key, value, ttl)
}

func (lease *redisProjectAuditLease) publish(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := lease.validatePublication(ctx, key); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return lease.publishRaw(ctx, key, raw, ttl)
}

func (lease *redisProjectAuditLease) validatePublication(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("storage audit cache publication requires a key")
	}
	if err := lease.ctx.Err(); err != nil {
		if lossErr := lease.currentLoss(); lossErr != nil {
			return lossErr
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (lease *redisProjectAuditLease) publishRaw(ctx context.Context, key string, raw []byte, ttl time.Duration) error {
	if err := lease.validatePublication(ctx, key); err != nil {
		return err
	}
	var ttlMilliseconds int64
	if ttl > 0 {
		ttlMilliseconds = ttl.Milliseconds()
		if ttlMilliseconds == 0 {
			ttlMilliseconds = 1
		}
	}
	publishCtx, cancel := context.WithCancel(ctx)
	stopOnLeaseLoss := context.AfterFunc(lease.ctx, cancel)
	defer func() {
		stopOnLeaseLoss()
		cancel()
	}()
	result, err := lease.client.Eval(publishCtx, projectAuditPublishLua, []string{lease.key, key}, lease.owner, raw, ttlMilliseconds).Int()
	if err != nil {
		if lease.ctx.Err() != nil {
			if lossErr := lease.currentLoss(); lossErr != nil {
				return lossErr
			}
			return lease.ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if lossErr := lease.currentLoss(); lossErr != nil {
			return lossErr
		}
		wrappedErr := fmt.Errorf("publish storage audit cache value: %w", err)
		lease.lose(wrappedErr)
		return wrappedErr
	}
	if result != 1 {
		lease.lose(ErrProjectAuditLeaseLost)
		return ErrProjectAuditLeaseLost
	}
	return nil
}

func (lease *redisProjectAuditLease) Release(ctx context.Context) error {
	lease.releaseOnce.Do(func() {
		lease.renewCancel()
		<-lease.renewDone
		_, lease.releaseErr = lease.client.Eval(ctx, projectAuditReleaseLua, []string{lease.key}, lease.owner).Int()
		if lease.releaseErr != nil {
			lease.cancel()
			lease.releaseErr = fmt.Errorf("release storage audit project lease: %w", lease.releaseErr)
		}
		lease.cancel()
		close(lease.releaseDone)
	})
	<-lease.releaseDone
	return lease.releaseErr
}

func (lease *redisProjectAuditLease) lose(err error) {
	lease.lossMu.Lock()
	if lease.lossErr == nil {
		lease.lossErr = err
	}
	lease.lossMu.Unlock()
	lease.cancel()
	lease.renewCancel()
}

func (lease *redisProjectAuditLease) currentLoss() error {
	lease.lossMu.Lock()
	defer lease.lossMu.Unlock()
	return lease.lossErr
}
