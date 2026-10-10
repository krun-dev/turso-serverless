package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	ps "turso-pageserver"
)

type databases []string

func (d *databases) String() string     { return strings.Join(*d, ",") }
func (d *databases) Set(v string) error { *d = append(*d, v); return nil }
func printJSON(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}
func main() {
	_, err := ps.Guard(func() bool { run(); return true })
	if err != nil {
		printJSON(map[string]any{"error": err.Error()})
		os.Exit(1)
	}
}
func run() {
	if len(os.Args) < 2 {
		panic("usage: pageserver server|cold|cloud|archive|controller [options]")
	}
	mode := os.Args[1]
	args := os.Args[2:]
	action := ""
	if mode == "archive" {
		if len(args) == 0 {
			panic("archive requires flush|watch|restore|compact|inspect")
		}
		action = args[0]
		args = args[1:]
	}
	f := flag.NewFlagSet(mode, flag.ContinueOnError)
	root := f.String("root", "", "local state root")
	port := f.Int64("port", 0, "loopback RPC port")
	service := f.String("service", "s3", "object backend: s3 or memory")
	endpoint := f.String("endpoint", os.Getenv("S3_ENDPOINT"), "S3-compatible endpoint (S3_ENDPOINT)")
	bucket := f.String("bucket", os.Getenv("S3_BUCKET"), "bucket (S3_BUCKET)")
	prefix := f.String("prefix", os.Getenv("S3_PREFIX"), "object prefix (S3_PREFIX)")
	region := f.String("region", ps.StorageRegion(), "signing region (S3_REGION)")
	createMode := f.String("create-mode", "s3", "atomic create: s3 or oss-forbid-overwrite")
	group := f.String("group", "", "archive group")
	database := f.String("database", "", "database ID")
	state := f.String("state", "", "coordinator directory")
	destination := f.String("destination", "", "new restore root")
	revision := f.Int64("revision", 0, "archive revision (0 means latest)")
	interval := f.Duration("interval", 5*time.Second, "archive watch interval")
	unsafeLatest := f.Bool("unsafe-latest", false, "test negative control")
	unsafeRecovery := f.Bool("unsafe-raw-recovery", false, "test negative control")
	oss := f.Bool("oss", false, "test OSS S3-compatible backend")
	var dbs databases
	f.Var(&dbs, "db", "ID=LOCAL_ROOT (repeatable)")
	if err := f.Parse(args); err != nil {
		panic(err)
	}
	if f.NArg() != 0 {
		panic("unexpected positional arguments")
	}
	connect := func() *ps.ObjectStorage {
		if *oss {
			return ps.ConnectWithMode("s3", *endpoint, *bucket, *prefix, *region, "oss-forbid-overwrite")
		}
		if *service == "oss" {
			return ps.Connect(*service, *endpoint, *bucket, *prefix, *region)
		}
		return ps.ConnectWithMode(*service, *endpoint, *bucket, *prefix, *region, *createMode)
	}
	var server ps.Service
	ready := map[string]any{}
	switch mode {
	case "server":
		if *root == "" {
			panic("--root required")
		}
		server = ps.NewStore(*root, *unsafeLatest, *unsafeRecovery)
	case "controller":
		server = ps.NewController()
	case "cloud":
		if *root == "" {
			panic("--root required")
		}
		if !*oss {
			*service = "memory"
		}
		storage := connect()
		storage.Probe()
		server = ps.NewCloudRouter(*root, storage)
	case "cold":
		if *root == "" || *group == "" || *database == "" {
			panic("--root, --group and --database required")
		}
		archive := ps.NewArchive(connect(), *group, filepath.Join(*root, "coordinator"))
		defer archive.Close()
		head, manifest := archive.Load(*revision)
		if head == nil {
			panic("no archive")
		}
		if *revision > 0 {
			raw, err := os.ReadFile(filepath.Join(*root, "cold-bootstrap.json"))
			if err == nil {
				var pinned struct {
					Manifest struct {
						Revision int64 `json:"revision"`
					} `json:"manifest"`
				}
				if err = json.Unmarshal(raw, &pinned); err != nil {
					panic(err)
				}
				if pinned.Manifest.Revision != *revision {
					panic("changing the requested revision requires a fresh cold root")
				}
			} else if !os.IsNotExist(err) {
				panic(err)
			}
		}
		dbs := manifest["databases"].(map[string]any)
		db, ok := dbs[*database].(map[string]any)
		if !ok {
			panic("archive database missing")
		}
		store := ps.NewColdStore(*root, archive, manifest, db, "")
		server = store
		ready["revision"] = store.CloudManifest["revision"]
		ready["catalog_revision"] = head["revision"]
		g := store.Generations[len(store.Generations)-1].(map[string]any)
		ready["local_position"] = map[string]any{"generation": g["number"], "epoch": g["epoch"], "frame": g["commit_frame"]}
	case "archive":
		if *state == "" || *group == "" {
			panic("--state and --group required")
		}
		storage := connect()
		storage.Probe()
		archive := ps.NewArchive(storage, *group, *state)
		defer archive.Close()
		var result any
		switch action {
		case "flush", "watch":
			roots := map[string]string{}
			for _, entry := range dbs {
				id, path, ok := strings.Cut(entry, "=")
				if !ok {
					panic("invalid --db")
				}
				roots[id] = path
			}
			if len(roots) == 0 {
				panic("at least one --db required")
			}
			if action == "watch" && *interval <= 0 {
				panic("interval must be positive")
			}
			for {
				result = archive.Flush(roots, nil)
				if action != "watch" {
					break
				}
				printJSON(map[string]any{"result": result, "operations": storage.Metrics()})
				time.Sleep(*interval)
			}
		case "restore":
			if *destination == "" {
				panic("--destination required")
			}
			result = archive.Restore(*destination, *revision)
		case "compact":
			result = archive.Compact()
		case "inspect":
			head, _ := archive.Load(*revision)
			result = head
		default:
			panic("unknown archive action")
		}
		printJSON(map[string]any{"result": result, "operations": storage.Metrics()})
		return
	default:
		panic("unknown service mode")
	}
	listener := ps.Listen(*port)
	defer listener.Close()
	ready["port"] = ps.Port(listener)
	printJSON(ready)
	if err := ps.Serve(listener, server); err != nil {
		panic(err)
	}
}
