package pageserver

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestExternalStorageConfiguration(t *testing.T) {
	t.Setenv("S3_PREFIX", "")
	expectFailure(func() { StorageRunPrefix("case-1") }, "S3_PREFIX is required")
	t.Setenv("S3_PREFIX", "/isolated/tests/")
	if got := StorageRunPrefix("unique/cold"); got != "isolated/tests/runs/unique/cold" {
		t.Fatalf("unexpected run prefix: %s", got)
	}
	expectFailure(func() { StorageRunPrefix("../escape") }, "")
	t.Setenv("S3_REGION", "")
	if StorageRegion() != "us-east-1" {
		t.Fatal("missing default signing region")
	}
	t.Setenv("S3_REGION", "test-region")
	if StorageRegion() != "test-region" {
		t.Fatal("configured signing region ignored")
	}
	for _, config := range []struct{ endpoint, bucket, prefix, message string }{
		{"", "example-bucket", "tests", "endpoint is required"},
		{"https://s3.example.invalid", "", "tests", "bucket is required"},
		{"https://s3.example.invalid", "example-bucket", "///", "prefix is required"},
	} {
		expectFailure(func() { ConnectWithMode("s3", config.endpoint, config.bucket, config.prefix, StorageRegion(), "s3") }, config.message)
	}
}

// Exercise the signed wire request and provider conflict response, rather than
// mocking the SDK. Both profiles must permit exactly one concurrent creator.
func TestS3AtomicCreateWire(t *testing.T) {
	for _, profile := range []string{"s3", "oss-forbid-overwrite"} {
		t.Run(profile, func(t *testing.T) {
			var mu sync.Mutex
			objects := map[string][]byte{}
			var invalid atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
					invalid.Add(1)
				}
				key := r.URL.Path
				switch r.Method {
				case "PUT":
					if profile == "s3" {
						if r.Header.Get("If-None-Match") != "*" || r.Header.Get("x-oss-forbid-overwrite") != "" {
							invalid.Add(1)
						}
					} else {
						if r.Header.Get("x-oss-forbid-overwrite") != "true" || r.Header.Get("If-None-Match") != "" || !strings.Contains(r.Header.Get("Authorization"), "x-oss-forbid-overwrite") {
							invalid.Add(1)
						}
					}
					raw, _ := io.ReadAll(r.Body)
					if _, ok := objects[key]; ok {
						code := "PreconditionFailed"
						status := 412
						if profile != "s3" {
							code = "FileAlreadyExists"
							status = 409
						}
						w.Header().Set("Content-Type", "application/xml")
						w.WriteHeader(status)
						fmt.Fprintf(w, "<Error><Code>%s</Code><Message>exists</Message></Error>", code)
						return
					}
					objects[key] = raw
				case "GET":
					raw, ok := objects[key]
					if !ok {
						w.WriteHeader(404)
						fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
						return
					}
					if r.Header.Get("Range") != "" {
						if r.Header.Get("Range") != "bytes=1-3" {
							invalid.Add(1)
						}
						w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-3/%d", len(raw)))
						w.WriteHeader(206)
						raw = raw[1:4]
					}
					w.Write(raw)
				case "DELETE":
					delete(objects, key)
					w.WriteHeader(204)
				default:
					invalid.Add(1)
					w.WriteHeader(405)
				}
			}))
			defer server.Close()
			client := s3.New(s3.Options{Region: "test-region", BaseEndpoint: aws.String(server.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test-access", "test-secret", ""), RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
			backend := &SDKBackend{s3: client, bucket: "bucket", prefix: "test", createMode: profile}
			var winners atomic.Int64
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ok, err := backend.Write("commit", []byte("abcdef"), true, false)
					if err != nil {
						t.Errorf("create: %v", err)
					}
					if ok {
						winners.Add(1)
					}
				}()
			}
			wg.Wait()
			if winners.Load() != 1 {
				t.Fatalf("winners = %d", winners.Load())
			}
			got, err := backend.Read("commit", 1, 3)
			if err != nil || !bytes.Equal(got, []byte("bcd")) {
				t.Fatalf("range: %q %v", got, err)
			}
			got, err = backend.Read("commit", -1, 0)
			if err != nil || string(got) != "abcdef" {
				t.Fatalf("original changed: %q %v", got, err)
			}
			if err := backend.Delete("commit"); err != nil {
				t.Fatal(err)
			}
			got, err = backend.Read("commit", -1, 0)
			if err != nil || got != nil {
				t.Fatalf("missing: %q %v", got, err)
			}
			if invalid.Load() != 0 {
				t.Fatalf("invalid signed requests = %d", invalid.Load())
			}
		})
	}
}

func TestPackRejectsCorruptAndTrailingData(t *testing.T) {
	raw := bytes.Repeat([]byte("page data"), 1000)
	sha := digest(raw)
	pack, index := makePack(map[string][]byte{sha: raw})
	extent := doc(index[sha])
	extent["key"] = "packs/test.pack"
	backend := NewMemoryBackend()
	backend.Write("packs/test.pack", pack, false, false)
	archive := NewArchive(NewStorage(backend), "test", t.TempDir())
	defer archive.Close()
	if got := archive.ReadChunk(sha, extent); !bytes.Equal(got, raw) {
		t.Fatal("pack roundtrip differs")
	}
	for _, kind := range []string{"checksum", "trailing", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			e := doc(clone(extent))
			b := bytes.Clone(pack)
			switch kind {
			case "checksum":
				b[num(e["offset"])] ^= 0xff
			case "trailing":
				e["length"] = num(e["length"]) + 1
			case "oversize":
				e["raw_length"] = ChunkSize + 1
			}
			backend.Write("packs/test.pack", b, false, false)
			_, err := Guard(func() []byte { return archive.ReadChunk(sha, e) })
			if err == nil {
				t.Fatal("invalid pack accepted")
			}
		})
	}
}
