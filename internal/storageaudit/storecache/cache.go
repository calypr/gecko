package storecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	// ProjectBucketInventoryRefreshInterval controls when ordinary requests ask
	// Syfon for a newer inventory. Forced refreshes fail instead of using stale data.
	ProjectBucketInventoryRefreshInterval = 10 * time.Minute
	ProjectBucketInventoryStaleTTL        = 24 * time.Hour
	ResponseCacheKeyPrefix                = "gecko:storage_chain_audit:v4:"
	ExactJoinCacheKeyPrefix               = "gecko:storage_exact_join:v1:"
	BucketInventoryKeyPrefix              = "gecko:project_bucket_inventory:v2:"
	StorageObservationKeyPrefix           = "gecko:storage_chain_observation:v1:"
	defaultExactJoinCacheTTL              = 5 * time.Minute
)

var ErrCacheOperation = errors.New("storage audit cache operation failed")

type ResponseCache interface {
	Get(ctx context.Context, key string) (CachedResponse, bool, error)
	Set(ctx context.Context, key string, value CachedResponse) error
	Source() string
}

type CachedResponse struct {
	CachedAt              time.Time                 `json:"cached_at"`
	RefreshDurationMillis int64                     `json:"refresh_duration_ms,omitempty"`
	Response              model.ChainAuditResponse  `json:"response"`
	Inventory             []model.RepoInventoryFile `json:"git_inventory,omitempty"`
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

type StorageObservationCache interface {
	GetMany(ctx context.Context, organization string, project string, version StorageObservationVersion, keys []string) (map[string]CachedStorageObservation, error)
	EnsureFences(ctx context.Context, organization string, project string, version StorageObservationVersion, keys []string) error
	SetMany(ctx context.Context, organization string, project string, writes []StorageObservationWrite) error
	NextVersion(ctx context.Context, organization string, project string) (StorageObservationVersion, error)
	Source() string
}

type StorageObservationVersion struct {
	Epoch   string `json:"epoch"`
	Counter uint64 `json:"counter"`
}

type CachedStorageObservation struct {
	Version    StorageObservationVersion                `json:"version"`
	ObservedAt string                                   `json:"observed_at"`
	Retired    bool                                     `json:"retired,omitempty"`
	Result     gintegrationsyfon.BulkStorageProbeResult `json:"result,omitempty"`
}

type StorageObservationWrite struct {
	Key         string
	FenceKey    string
	Observation CachedStorageObservation
}

func NewStorageChainAuditResponseCacheFromEnv() (ResponseCache, error) {
	redisURL, err := requiredStorageAuditRedisURLFromEnv()
	if err != nil {
		return nil, err
	}
	cache, err := NewRedisStorageChainAuditResponseCache(redisURL)
	if err != nil {
		return nil, fmt.Errorf("initialize storage chain audit response cache: %w", err)
	}
	return cache, nil
}

func NewStorageExactProjectJoinCacheFromEnv() (ExactJoinCache, error) {
	redisURL, err := requiredStorageAuditRedisURLFromEnv()
	if err != nil {
		return nil, err
	}
	cache, err := NewRedisStorageExactProjectJoinCache(redisURL)
	if err != nil {
		return nil, fmt.Errorf("initialize storage exact project join cache: %w", err)
	}
	return cache, nil
}

func NewProjectBucketInventoryCacheFromEnv() (BucketInventoryCache, error) {
	redisURL, err := requiredStorageAuditRedisURLFromEnv()
	if err != nil {
		return nil, err
	}
	cache, err := NewRedisProjectBucketInventoryCache(redisURL)
	if err != nil {
		return nil, fmt.Errorf("initialize project bucket inventory cache: %w", err)
	}
	return cache, nil
}

func NewStorageObservationCacheFromEnv() (StorageObservationCache, error) {
	redisURL, err := requiredStorageAuditRedisURLFromEnv()
	if err != nil {
		return nil, err
	}
	cache, err := NewRedisStorageObservationCache(redisURL)
	if err != nil {
		return nil, fmt.Errorf("initialize storage observation cache: %w", err)
	}
	return cache, nil
}

func requiredStorageAuditRedisURLFromEnv() (string, error) {
	if raw := strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_ENABLED")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return "", fmt.Errorf("STORAGE_CHAIN_AUDIT_CACHE_ENABLED must be true: %w", err)
		}
		if !enabled {
			return "", errors.New("STORAGE_CHAIN_AUDIT_CACHE_ENABLED=false is unsupported; storage audit caches require Redis")
		}
	}
	redisURL := StorageChainAuditRedisURLFromEnv()
	if redisURL == "" {
		return "", errors.New("STORAGE_CHAIN_AUDIT_CACHE_REDIS_URL or REDIS_URL is required")
	}
	return redisURL, nil
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

func WrapCacheOperationError(source string, operation string, err error) error {
	if err == nil {
		return nil
	}
	return cacheOperationError{source: strings.TrimSpace(source), operation: strings.TrimSpace(operation), err: err}
}

type cacheOperationError struct {
	source    string
	operation string
	err       error
}

func (err cacheOperationError) Error() string {
	return fmt.Sprintf("%s cache %s: %v", err.source, err.operation, err.err)
}

func (err cacheOperationError) Unwrap() error { return err.err }

func (err cacheOperationError) Is(target error) bool { return target == ErrCacheOperation }

func IsCacheOperationError(err error) bool {
	return errors.Is(err, ErrCacheOperation)
}

func StorageExactProjectJoinCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("STORAGE_EXACT_PROJECT_JOIN_CACHE_TTL_SECONDS"))
	if raw == "" {
		// Keep the old setting as a deprecated alias for exact-join state only.
		raw = strings.TrimSpace(os.Getenv("STORAGE_CHAIN_AUDIT_CACHE_TTL_SECONDS"))
	}
	if raw == "" {
		return defaultExactJoinCacheTTL
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 60 {
		return defaultExactJoinCacheTTL
	}
	return time.Duration(seconds) * time.Second
}

func StorageChainAuditResponseCacheKey(organization string, project string, ref string, gitSubpath string, probeMode string, validationMode string, bucketMode string, bucketPathPrefix string) string {
	body := fmt.Sprintf(
		"org=%s\nproject=%s\nref=%s\ngit_subpath=%s\nprobe_mode=%s\nvalidation_mode=%s\nbucket_mode=%s\nbucket_path_prefix=%s",
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(ref),
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

func StorageObservationCacheKey(organization string, project string, operation string, canonicalURL string, expectedSize string, expectedChecksum string, expectedName string, mappingIdentity string, inventoryFingerprint string) string {
	return storageObservationKey("observation", organization, project, operation, canonicalURL, expectedSize, expectedChecksum, expectedName, mappingIdentity, inventoryFingerprint)
}

func StorageObservationRetirementKey(organization string, project string, canonicalURL string, mappingIdentity string) string {
	return storageObservationKey("retirement", organization, project, "", canonicalURL, "", "", "", mappingIdentity, "")
}

func StorageObservationVersionKey(organization string, project string) string {
	sum := hashStorageObservationIdentity("version", strings.TrimSpace(organization), strings.TrimSpace(project))
	return StorageObservationKeyPrefix + "version:" + hex.EncodeToString(sum[:])
}

func storageObservationKey(kind string, organization string, project string, operation string, canonicalURL string, expectedSize string, expectedChecksum string, expectedName string, mappingIdentity string, inventoryFingerprint string) string {
	sum := hashStorageObservationIdentity(
		kind,
		strings.TrimSpace(organization),
		strings.TrimSpace(project),
		strings.TrimSpace(operation),
		strings.TrimSpace(canonicalURL),
		strings.TrimSpace(expectedSize),
		strings.TrimSpace(expectedChecksum),
		strings.TrimSpace(expectedName),
		strings.TrimSpace(mappingIdentity),
		strings.TrimSpace(inventoryFingerprint),
	)
	return StorageObservationKeyPrefix + hex.EncodeToString(sum[:])
}

func hashStorageObservationIdentity(fields ...string) [32]byte {
	var identity strings.Builder
	for _, field := range fields {
		identity.WriteString(strconv.Itoa(len(field)))
		identity.WriteByte(':')
		identity.WriteString(field)
	}
	return sha256.Sum256([]byte(identity.String()))
}
