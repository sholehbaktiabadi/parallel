# Changelog

## v0.1.0

First tagged release.

**If you used this module before the tag** — that is, through a pseudo-version of the
default branch — two things changed underneath you.

### `RunWith` and `MapWith` now hand your function a context

This is what makes them a replacement for `errgroup.WithContext`: the context they give you
is cancelled when `Options.Context` is cancelled and, with `StopOnError`, as soon as any
task fails. Pass it into whatever you call and work that is already running stops itself.

```go
// before
parallel.RunWith(opt, func() error { ... })
parallel.MapWith(opt, items, func(item T) (R, error) { ... })

// after
parallel.RunWith(opt, func(ctx context.Context) error { ... })
parallel.MapWith(opt, items, func(ctx context.Context, item T) (R, error) { ... })
```

`Run` and `Map` are unchanged — they still take plain functions with no context.

### The minimum Go version is 1.22

It was briefly 1.25, because the goroutines were started with `sync.WaitGroup.Go`. They now
use `wg.Add(1)` and `defer wg.Done()` instead, which costs two lines and buys back every
toolchain from 1.22 onwards. Nothing else in the library needs anything newer.

### Also in this release

- Cancelled and stopped runs report the tasks that never started **once**, as a single
  summary, instead of one error per task. A cancelled batch of a million tasks used to
  return a 17 MB error message; it is now 82 bytes.
- Tasks skipped because of `StopOnError` are reported as `ErrStopped`, not
  `context.Canceled`, so "my caller went away" can no longer be confused with "one of my
  tasks failed".
- A task that ends via `runtime.Goexit` — what `t.Fatal` and testify's `require.*` do — is
  reported as `ErrIncomplete` instead of being silently counted as a success.
- Recovered panics are wrapped so `errors.Is(err, ErrPanic)` works.
