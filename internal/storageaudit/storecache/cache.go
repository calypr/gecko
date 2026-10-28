package storecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	gintegrationsyfon "github.com/calypr/gecko/internal/integrations/syfon"
	"github.com/calypr/gecko/internal/storageaudit/model"
	"github.com/calypr/gecko/internal/storageaudit/record"
)

const (
	DefaultResponseCacheTTL = 5 * time.Minute
	// ProjectBucketInventoryRefreshInterval controls when Gecko asks Syfon for
	// a newer inventory. The cache retains the last successful result longer so
	// an intermittent provider failure does not turn into a user-facing 502.
	ProjectBucketInventoryRefreshInterval = 10 * time.Minute
	ProjectBucketInventoryStaleTTL        = 24 * time.Hour
	ResponseCacheKeyPrefix                = "gecko:storage_chain_audit:v3:"
	ExactJoinCacheKeyPrefix               = "gecko:storage_exact_join:v1:"
	BucketInventoryKeyPrefix              = "gecko:project_bucket_inventory:v2:"
)

type ResponseCache interface {
	Get(ctx context.Context, key string) (CachedResponse, bool, error)
	Set(ctx context.Context, key string, value CachedResponse, ttl time.Duration) error
	Source() string
}

type CachedResponse struct {
	CachedAt              time.Time                `json:"cached_at"`
	RefreshDurationMillis int64                    `json:"refresh_duration_ms,omitempty"`
	Response              model.ChainAuditResponse `json:"response"`
}

type ExactJoinCache interface {
	Get(ctx context.Context, key string) (CachedExactJoinState, bool, error)
	Set(ctx context.Context, key string, value CachedExactJoinState, ttl time.Duration) error
	Source() string
}

type CachedExactJoinState struct {
	CachedAt          time.Time                              `json:"cached_at"`
	RecordsByChecksum map[string][]record.State              `json:"records_by_checksum"`
	UsageByObjectID   map[string]gintegrationsyfon.FileUsage `json:"usage_by_object_id"`
}

type BucketInventoryCache interface {
	Get(ctx context.Context, key string) (CachedBucketInventory, bool, error)
	Set(ctx context.Context, key string, value CachedBucketInventory, ttl time.Duration) error
	Source() string
}

type CachedBucketInventory struct {
	CachedAt time.Time                               `json:"cached_at"`
	Objects  []gintegrationsyfon.ProjectBucketObject `json:"objects"`
	Complete bool                                    `json:"complete"`
	Warning  string                                  `json:"warning,omitempty"`
	Observed string                                  `json:"observed_at,omitempty"`
}

func NewStorageChainAuditResponseCacheFromEnv() ResponseCache {
	if !StorageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := NewRedisStorageChainAuditResponseCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return NewMemoryStorageChainAuditResponseCache()
}

func NewStorageExactProjectJoinCacheFromEnv() ExactJoinCache {
	if !StorageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := NewRedisStorageExactProjectJoinCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return NewMemoryStorageExactProjectJoinCache()
}

func NewProjectBucketInventoryCacheFromEnv() BucketInventoryCache {
	if !StorageChainAuditCacheEnabled() {
		return nil
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL != "" {
		cache, err := NewRedisProjectBucketInventoryCache(redisURL)
		if err == nil {
			return cache
		}
	}
	return NewMemoryProjectBucketInventoryCache()
}

func StorageChainAuditRedisURLFromEnv() string {
	redisURL := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL"))
	if redisURL == "" {
		redisURL = strings.TrimSpace(os.Getenv("REDIS_URL"))
	}
	return RedisURLWithPassword(redisURL, strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_REDIS_PASSWORD")))
}

func RedisURLWithPassword(redisURL string, password string) string {
	redisURL = strings.TrimSpace(redisURL)
	password = strings.TrimSpace(password)
	if redisURL == "" || password == "" || !strings.HasPrefix(redisURL, "redis://") || strings.Contains(redisURL, "@") {
		return redisURL
	}
	return "redis://:" + password + "@" + strings.TrimPrefix(redisURL, "redis://")
}

func StorageChainAuditCacheEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED"))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	return err == nil && enabled
}

func StorageChainAuditCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_TTL_SECONDS"))
	if raw == "" {
		return DefaultResponseCacheTTL
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 60 {
		return DefaultResponseCacheTTL
	}
	return time.Duration(seconds) * time.Second
}

func StorageChainAuditResponseCacheKey(organization string, project string, ref string, gitSubpath string, probeMode string, validationMode string, bucketMode string, bucketPathPrefix string, hash string, syfonRevision string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nref=%s\nhash=%s\nsyfon_revision=%s\ngit_subpath=%s\nprobe_mode=%s\nvalidation_mode=%s\nbucket_mode=%s\nbucket_path_prefix=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(ref),
		strings.TrimSpace(hash),
		strings.TrimSpace(syfonRevision),
		record.NormalizeRepoSubpath(gitSubpath),
		strings.TrimSpace(probeMode),
		strings.TrimSpace(validationMode),
		strings.TrimSpace(bucketMode),
		record.NormalizeRepoSubpath(bucketPathPrefix),
	)
	sum := sha256.Sum256([]byte(body))
	return ResponseCacheKeyPrefix + hex.EncodeToString(sum[:])
}

func StorageExactProjectJoinCacheKey(organization string, project string, hash string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nhash=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(hash),
	)
	sum := sha256.Sum256([]byte(body))
	return ExactJoinCacheKeyPrefix + hex.EncodeToString(sum[:])
}

func ProjectBucketInventoryCacheKey(organization string, project string, bucketPathPrefix string) string {
	body := fmt.Sprintf("org=%s\nproject=%s\nbucket_path_prefix=%s", strings.TrimSpace(organization), strings.TrimSpace(project), record.NormalizeRepoSubpath(bucketPathPrefix))
	sum := sha256.Sum256([]byte(body))
	return BucketInventoryKeyPrefix + hex.EncodeToString(sum[:])
}
