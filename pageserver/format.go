package pageserver

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"unicode/utf16"
)

// Canonical catalog JSON sorts keys, omits separator whitespace and escapes
// non-ASCII characters. These bytes form the persistent checksum contract.
type Document = map[string]any

type operationError struct{ Kind, Message string }

func (e operationError) Error() string { return e.Message }
func require(ok bool, message string) {
	if !ok {
		panic(operationError{"IntegrityError", message})
	}
}
func fail(kind, message string) { panic(operationError{kind, message}) }
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Guard keeps failed invariants and filesystem/backend errors inside an RPC or
// CLI operation. Deferred unlock/cleanup still run before returning the error.
func Guard[T any](f func() T) (value T, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	return f(), nil
}
func decode(raw []byte) any {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return mustValue(d)
}
func mustValue(d *json.Decoder) any { var v any; check(d.Decode(&v)); return v }
func doc(v any) Document {
	if v == nil {
		return Document{}
	}
	return v.(map[string]any)
}
func list(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}
func num(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int:
		return int64(x)
	case int64:
		return x
	case uint32:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		return must(x.Int64())
	default:
		panic(fmt.Errorf("invalid number %T", v))
	}
}
func str(v any) string {
	if v == nil {
		return ""
	}
	return v.(string)
}
func flag(v any) bool {
	if v == nil {
		return false
	}
	return v.(bool)
}
func encode(v any) []byte {
	raw := must(json.Marshal(v))
	normal := decode(raw)
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	check(e.Encode(normal))
	var out strings.Builder
	for _, r := range strings.TrimSuffix(b.String(), "\n") {
		if r < 128 {
			out.WriteRune(r)
		} else if r <= 0xffff {
			fmt.Fprintf(&out, "\\u%04x", r)
		} else {
			a, z := utf16.EncodeRune(r)
			fmt.Fprintf(&out, "\\u%04x\\u%04x", a, z)
		}
	}
	return []byte(out.String())
}
func clone(v any) any          { return decode(encode(v)) }
func equal(a, b any) bool      { return bytes.Equal(encode(a), encode(b)) }
func digest(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func randomID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	check(err)
	return hex.EncodeToString(b)
}
func exists(p string) bool {
	_, err := os.Stat(p)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	panic(err)
}
func readFile(p string) []byte { return must(os.ReadFile(p)) }
func atomicWrite(path string, raw []byte) {
	check(os.MkdirAll(filepath.Dir(path), 0700))
	temp := path + "." + randomID() + ".tmp"
	f := must(os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600))
	defer func() { f.Close(); os.Remove(temp) }()
	_, err := f.Write(raw)
	check(err)
	check(fsync(f))
	check(f.Close())
	check(os.Rename(temp, path))
	syncDir(filepath.Dir(path))
}

// fsync preserves the prototype POSIX contract on all supported Unix hosts.
// Physical power-loss guarantees require separate validation.
func fsync(f *os.File) error { return syscall.Fsync(int(f.Fd())) }
func syncDir(path string)    { f := must(os.Open(path)); defer f.Close(); check(fsync(f)) }
func safePath(path string) string {
	require(path != "" && path != "." && !filepath.IsAbs(path), "invalid database object path")
	for _, p := range strings.Split(filepath.ToSlash(path), "/") {
		require(p != "..", "invalid database object path")
	}
	return filepath.ToSlash(filepath.Clean(path))
}

var identityPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)

func identity(name string) string {
	require(identityPattern.MatchString(name), "invalid database/group identity")
	return name
}
func fileRange(path string, offset, length int64) []byte {
	f := must(os.Open(path))
	defer f.Close()
	info := must(f.Stat())
	end := min(info.Size(), offset+length)
	if offset >= end {
		return []byte{}
	}
	b := make([]byte, end-offset)
	_, err := f.ReadAt(b, offset)
	if err != nil && err != io.EOF {
		panic(err)
	}
	return b
}
