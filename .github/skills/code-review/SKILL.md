---
name: code-review
description: Review pull requests and diffs for the RabbitMQ Stream Go Client. Use when reviewing a PR, a branch diff, or a set of changed .go files in this repository, to check concurrency safety, error handling, resource lifecycle, and lint/test compliance specific to this codebase.
---

# RabbitMQ Stream Go Client — Code Review

Review Go changes in this repository against the conventions in `AGENTS.md` and the
package layout under `pkg/`. Read the diff first, then only open the surrounding
file context needed to judge each change (existing patterns in the same file/package
usually settle style questions).

## Scope check

Identify which packages are touched, since expectations differ:

- `pkg/stream/` — core client (`environment.go`, `producer.go`, `consumer.go`,
  `client.go`, `super_stream_producer.go`, `super_stream_consumer.go`). Highest bar:
  concurrency and connection-lifecycle correctness.
- `pkg/ha/` — `ReliableProducer` / `ReliableConsumer`. Reconnect and retry logic must
  not regress; check that failures surface via `NotifyClose()`-style channels rather
  than being swallowed.
- `pkg/amqp/` — AMQP 1.0 encode/decode. Watch for buffer/index bugs and malformed
  input handling.
- `pkg/message/`, `pkg/logs/` — smaller surfaces, lower risk.
- `examples/`, `perfTest/` — verify they still compile against any API changes.
- `pkg/integration_test/`, `pkg/test-helper/` — tests requiring a running broker.

## Correctness checklist

1. **Mutex discipline** — mutexes must never be held across network I/O or channel
   sends that can block. Flag any lock held across a `conn.Write`, `conn.Read`, RPC
   call, or unbuffered channel send/receive.
2. **Message sharing** — messages are explicitly **not** thread-safe (see AGENTS.md).
   Flag any code path that stores/reuses/mutates a `message.StreamMessage` across
   goroutines, or that shares a message between concurrent `Send`/`BatchSend` calls.
3. **Connection/producer/consumer lifecycle** — these are meant to be long-lived.
   Flag code that opens/closes a `Client`, `Producer`, or `Consumer` per-message or
   in a hot loop instead of reusing an existing instance.
4. **Close/error propagation** — verify `NotifyClose()` (or equivalent) channels are
   read and handled, not ignored. Verify goroutines that own a channel close it
   exactly once and don't send after close (check for double-close panics and
   send-on-closed-channel races).
5. **Error handling** — check with `errors.Is()`/`errors.As()` against typed sentinel
   errors (e.g. `stream.StreamAlreadyExists`) instead of string comparison. Errors
   from `NewEnvironment()`, `NewProducer()`, `NewConsumer()` must be checked, not
   discarded.
6. **Goroutine lifecycle** — every started goroutine needs a clear exit condition
   tied to connection/consumer/producer close; flag goroutines with no shutdown path
   (leaks) or that can panic on a closed connection/channel.
7. **Context and cancellation** — if a function takes a `context.Context`, verify it
   is actually threaded through to blocking calls, not just accepted and ignored.

## Lint/style checklist (mirrors `.golangci.yml`)

- `errcheck` — no ignored error returns (including deferred `Close()` calls).
- `bodyclose` — HTTP/response bodies closed on every path, including error paths.
- `gosec` — no unsafe use of crypto/randomness beyond the repo's existing exclusions
  (G404, G115 are intentionally excluded here — don't re-flag those two).
- `ineffassign` / `unused` — no dead assignments or unused identifiers.
- `prealloc` — slices with a known final size built via `append` in a loop should be
  preallocated.
- `dupl` / `goconst` — flag near-duplicate blocks or repeated string literals that
  should be extracted, but only when it doesn't fight existing repo patterns.
- `gofmt`/`goimports` — flag unformatted code; note the repo rewrite rule
  `interface{}` → `any`.
- Naked returns (`nakedret`) in anything but very short functions.

## Testing checklist

- New behavior in `pkg/stream/` or `pkg/ha/` should have a unit test alongside the
  source file (`*_test.go`).
- Changes touching broker interaction should be covered (or explicitly noted as
  needing coverage) in `pkg/integration_test/`.
- Confirm `go test ./...` and `go vet ./...` are clean for touched packages; suggest
  running `golangci-lint run` (v2.6, per `.github/workflows/golangci-lint.yml`) when
  lint-sensitive code changed.

## Housekeeping to flag if missing

- User-facing behavior changes without a `CHANGELOG.md` entry.
- Public API changes without updated examples (`examples/`) or README sections.
- New reconnect/retry knobs not reflected in `best_practices/README.md` when they
  change recommended usage.

## Output

Report findings ranked by severity: correctness/concurrency bugs first, then
resource-lifecycle and error-handling issues, then lint/style, then test/doc gaps.
For each finding, cite `file:line`, state the concrete failure scenario (not just
"this could be a problem"), and suggest the minimal fix consistent with existing
patterns in the same file.
