# Engine memory diagnostics

Diagnostics are disabled by default. Enable them on an Engine with
`SERVER_DIAGNOSTICS_ENABLED=true`. The HTTP listener accepts only an explicit
loopback IP address; its default is `127.0.0.1:6060`. Do not create a Service or
Ingress for this port. Use `kubectl exec` or `kubectl port-forward` for access.

Endpoints accept GET requests:

- `/debug/memory`: current goroutines, heap, stack, Go-managed memory, and GC count.
- `/debug/pprof/goroutine`: binary goroutine profile; `?debug=1` returns grouped stacks.
- `/debug/pprof/heap`: sampled live heap profile, without forcing GC.
- `/debug/pprof/allocs`: cumulative sampled allocations.
- `/debug/pprof/threadcreate`: thread-creation stacks.

Timed profiles, forced GC, CPU tracing, and command-line disclosure are not
exposed. Concurrent profile requests return 503 while a capture is in progress.
Existing health/readiness probes are independent of diagnostics.

## Automatic capture

Set `SERVER_DIAGNOSTICS_SNAPSHOT_DIR` to a writable directory to enable a
baseline capture after the first sampling interval and subsequent threshold
captures. Mount a disk-backed `emptyDir` at that path to preserve snapshots
across container restarts. An `emptyDir` is removed when the Pod is replaced;
retrieve profiles before deleting it. A bounded grouped-stack summary is also
written to the normal application logger for threshold captures.

| Environment variable | Default |
| --- | --- |
| `SERVER_DIAGNOSTICS_ADDRESS` | `127.0.0.1:6060` |
| `SERVER_DIAGNOSTICS_INTERVAL` | `15s` |
| `SERVER_DIAGNOSTICS_COOLDOWN` | `5m` |
| `SERVER_DIAGNOSTICS_GOROUTINE_THRESHOLD` | `5000` |
| `SERVER_DIAGNOSTICS_MEMORY_THRESHOLD` | `1073741824` bytes |
| `SERVER_DIAGNOSTICS_MAX_SNAPSHOTS` | `4` |

Memory means Go's `MemStats.Sys - MemStats.HeapReleased`, not cgroup working set
or RSS. Either positive threshold triggers capture; zero disables that threshold.
Each snapshot contains `metadata.json`, `heap.pprof`, and `goroutine.pprof`.
Files are private to the process user. Each binary profile is limited to 32 MiB;
old snapshot directories are removed before a new capture, and failed captures
respect the cooldown. Four snapshots require at most approximately 256 MiB plus
small metadata files; a 512 MiB disk-backed volume leaves headroom.

For local analysis, copy a snapshot directory with `kubectl cp`, then run:

```sh
go tool pprof -top ./goroutine.pprof
go tool pprof -top -sample_index=inuse_space ./heap.pprof
go tool pprof -top -base ./baseline/heap.pprof ./incident/heap.pprof
```

Heap profiles are sampled and reflect the last completed GC. Compare several
snapshots with memory/goroutine metrics and task traffic before attributing a
leak. These probes collect evidence; they do not restart or repair the Engine.
