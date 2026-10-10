package pageserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Backend is intentionally smaller than a provider SDK. Publication needs
// atomic create; there is no head-then-put or LIST-based fallback.
type Backend interface {
	Read(string, int64, int64) ([]byte, error)
	Write(string, []byte, bool, bool) (bool, error)
	Delete(string) error
}
type MemoryBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func NewMemoryBackend() *MemoryBackend { return &MemoryBackend{objects: map[string][]byte{}} }
func (b *MemoryBackend) Read(k string, offset, length int64) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, ok := b.objects[k]
	if !ok {
		return nil, nil
	}
	if offset < 0 {
		return bytes.Clone(raw), nil
	}
	if offset > int64(len(raw)) {
		return nil, fmt.Errorf("short object range")
	}
	return bytes.Clone(raw[offset:min(int64(len(raw)), offset+length)]), nil
}
func (b *MemoryBackend) Write(k string, v []byte, create, hint bool) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.objects[k]; create && ok {
		return false, nil
	}
	b.objects[k] = bytes.Clone(v)
	return true, nil
}
func (b *MemoryBackend) Delete(k string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, k)
	return nil
}

type SDKBackend struct {
	s3                         *s3.Client
	bucket, prefix, createMode string
}

func (b *SDKBackend) key(k string) string { return strings.Trim(b.prefix, "/") + "/" + k }
func requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 60*time.Second)
}
func providerCode(err error) string {
	var a smithy.APIError
	if errors.As(err, &a) {
		return a.ErrorCode()
	}
	return ""
}
func (b *SDKBackend) Read(k string, offset, length int64) ([]byte, error) {
	ctx, cancel := requestContext()
	defer cancel()
	var body io.ReadCloser
	var err error
	var region *string
	if offset >= 0 {
		region = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}
	var result *s3.GetObjectOutput
	result, err = b.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(b.key(k)), Range: region})
	if err == nil {
		body = result.Body
	}

	if err != nil {
		if providerCode(err) == "NoSuchKey" || providerCode(err) == "NotFound" {
			return nil, nil
		}
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}
func (b *SDKBackend) Write(k string, raw []byte, create, hint bool) (bool, error) {
	ctx, cancel := requestContext()
	defer cancel()
	var err error
	var cache *string
	if hint {
		cache = aws.String("no-cache")
	}
	r := &s3.PutObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(b.key(k)), Body: bytes.NewReader(raw), ContentLength: aws.Int64(int64(len(raw))), CacheControl: cache}
	var opts []func(*s3.Options)
	if create {
		if b.createMode == "s3" {
			r.IfNoneMatch = aws.String("*")
		} else {
			opts = append(opts, s3.WithAPIOptions(func(stack *middleware.Stack) error {
				return stack.Build.Add(middleware.BuildMiddlewareFunc("AtomicCreate", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
					in.Request.(*smithyhttp.Request).Header.Set("x-oss-forbid-overwrite", "true")
					return next.HandleBuild(ctx, in)
				}), middleware.After)
			}))
		}
	}
	_, err = b.s3.PutObject(ctx, r, opts...)

	if create && (providerCode(err) == "FileAlreadyExists" || providerCode(err) == "PreconditionFailed" || providerCode(err) == "ConditionalRequestConflict") {
		return false, nil
	}
	return err == nil, err
}
func (b *SDKBackend) Delete(k string) error {
	ctx, cancel := requestContext()
	defer cancel()
	_, err := b.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(b.key(k))})
	return err
}

type ObjectStorage struct {
	Backend    Backend
	Identity   any
	mu         sync.Mutex
	counts     map[string]int64
	mapKey     func(string) string
	shared     *ObjectStorage
	knownPacks *sync.Map
	// Hooks are confined to the loopback test controller; CLI services leave
	// these empty. They exercise outages/corruption and publication barriers.
	RangeFault string
	PutBarrier string
}

func NewStorage(backend Backend) *ObjectStorage {
	return &ObjectStorage{Backend: backend, counts: map[string]int64{}}
}
func Connect(service, endpoint, bucket, prefix, region string) *ObjectStorage {
	mode := "s3"
	// Legacy fixture spelling; all cloud requests still use the S3 client.
	if service == "oss" {
		service = "s3"
		mode = "oss-forbid-overwrite"
		endpoint = strings.Replace(endpoint, "https://oss-", "https://s3.oss-", 1)
	}
	return ConnectWithMode(service, endpoint, bucket, prefix, region, mode)
}
func ConnectWithMode(service, endpoint, bucket, prefix, region, mode string) *ObjectStorage {
	if service == "memory" {
		return NewStorage(NewMemoryBackend())
	}
	require(service == "s3", "unsupported object service")
	require(mode == "s3" || mode == "oss-forbid-overwrite", "unsupported conditional creation mode")
	require(strings.TrimSpace(endpoint) != "", "S3 endpoint is required")
	require(strings.TrimSpace(bucket) != "", "S3 bucket is required")
	require(strings.Trim(prefix, "/ ") != "", "S3 object prefix is required")
	id, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	require(id != "" && secret != "", "object storage credentials are missing")
	b := &SDKBackend{bucket: bucket, prefix: prefix, createMode: mode}
	b.s3 = s3.New(s3.Options{Region: region, BaseEndpoint: aws.String(endpoint), UsePathStyle: false, Credentials: credentials.NewStaticCredentialsProvider(id, secret, ""), RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
	s := NewStorage(b)
	s.Identity = Document{"service": service, "endpoint": strings.TrimRight(endpoint, "/"), "bucket": bucket, "prefix": strings.Trim(prefix, "/"), "region": region, "create_mode": mode}
	return s
}

func StorageRegion() string {
	if region := os.Getenv("S3_REGION"); region != "" {
		return region
	}
	return "us-east-1"
}

// External tests always write beneath an explicitly configured base prefix.
func StorageRunPrefix(run string) string {
	prefix := strings.Trim(os.Getenv("S3_PREFIX"), "/")
	require(strings.TrimSpace(prefix) != "", "S3_PREFIX is required for external storage tests")
	safePath(prefix)
	safePath(run)
	return prefix + "/runs/" + run
}

func (s *ObjectStorage) count(op, k string) {
	require(k != "" && !strings.HasSuffix(k, "/") && !path.IsAbs(k), "invalid object key")
	safePath(k)
	s.mu.Lock()
	s.counts[op]++
	s.mu.Unlock()
}
func (s *ObjectStorage) Metrics() Document {
	if s.shared != nil {
		return s.shared.Metrics()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := Document{"LIST": 0, "metadata_db_operations": 0, "unit": "object API calls; SDK retries may add HTTP requests"}
	for k, v := range s.counts {
		r[k] = v
	}
	return r
}
func (s *ObjectStorage) Read(k string) []byte {
	if s.shared != nil {
		return s.shared.Read(s.mapKey(k))
	}
	s.count("GET", k)
	return must(s.Backend.Read(k, -1, 0))
}
func (s *ObjectStorage) ReadRange(k string, offset, length int64) []byte {
	if s.shared != nil {
		return s.shared.ReadRange(s.mapKey(k), offset, length)
	}
	s.mu.Lock()
	fault := s.RangeFault
	s.mu.Unlock()
	if fault == "outage" {
		fail("OSError", "injected object outage")
	}
	if fault == "corrupt" {
		return []byte("invalid compressed data")
	}
	require(offset >= 0 && length > 0, "invalid range")
	s.count("RANGE_GET", k)
	b := must(s.Backend.Read(k, offset, length))
	require(int64(len(b)) == length, "short object range")
	return b
}
func (s *ObjectStorage) Create(k string, raw []byte) bool {
	require(len(raw) <= 16*1024, "prototype only validates atomic creation for small commit records")
	if s.shared != nil {
		return s.shared.Create(s.mapKey(k), raw)
	}
	s.count("CREATE", k)
	return must(s.Backend.Write(k, raw, true, false))
}

var sharedPackLock sync.Mutex

func (s *ObjectStorage) Put(k string, raw []byte) {
	if s.shared != nil {
		mapped := s.mapKey(k)
		if strings.HasPrefix(k, "packs/") {
			sharedPackLock.Lock()
			defer sharedPackLock.Unlock()
			require(path.Base(mapped) == digest(raw)+".pack", "shared pack identity mismatch")
			if _, loaded := s.knownPacks.LoadOrStore(mapped, struct{}{}); loaded {
				return
			}
			defer func() {
				if r := recover(); r != nil {
					s.knownPacks.Delete(mapped)
					panic(r)
				}
			}()
		}
		s.shared.Put(mapped, raw)
		return
	}
	s.mu.Lock()
	barrier := s.PutBarrier
	s.mu.Unlock()
	if barrier != "" {
		atomicWrite(barrier+".entered", []byte{})
		deadline := time.Now().Add(20 * time.Second)
		for !exists(barrier + ".release") {
			require(time.Now().Before(deadline), "blocked upload timeout")
			time.Sleep(5 * time.Millisecond)
		}
	}
	s.count("PUT", k)
	must(s.Backend.Write(k, raw, false, false))
}
func (s *ObjectStorage) Hint(k string, raw []byte) {
	if s.shared != nil {
		s.shared.Hint(s.mapKey(k), raw)
		return
	}
	s.count("HINT_PUT", k)
	must(s.Backend.Write(k, raw, false, true))
}
func (s *ObjectStorage) Delete(k string) {
	if s.shared != nil {
		s.shared.Delete(s.mapKey(k))
		return
	}
	s.count("PROBE_DELETE", k)
	check(s.Backend.Delete(k))
}
func (s *ObjectStorage) Probe() {
	k := "probes/" + randomID()
	defer s.Delete(k)
	require(s.Create(k, []byte("first")), "probe already exists")
	require(!s.Create(k, []byte("second")) && bytes.Equal(s.Read(k), []byte("first")), "backend ignored atomic creation condition")
	require(bytes.Equal(s.ReadRange(k, 1, 3), []byte("irs")), "backend failed range probe")
}
func CaseStorage(s *ObjectStorage, namespace string, known *sync.Map) *ObjectStorage {
	return &ObjectStorage{shared: s, knownPacks: known, Identity: Document{"backend": s.Identity, "namespace": namespace}, mapKey: func(k string) string {
		if strings.HasPrefix(k, "packs/") {
			return "shared-packs/" + strings.Split(path.Base(k), "__")[0] + ".pack"
		}
		return "cases/" + namespace + "/" + k
	}}
}
