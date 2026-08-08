# parallel

Run Go functions at the same time, without touching goroutines, channels, `WaitGroup` or mutexes.

Two functions cover almost everything:

```go
parallel.Run(fn1, fn2, fn3)  // run these together, wait for all of them
parallel.Map(items, fn)      // run fn on every item together, collect the results
```

Everything else is one struct with three fields and three sentinel errors. The entire
implementation is a single file ([`parallel.go`](parallel.go)) written to be read — if you
have never written a goroutine before, reading it is the point.

## Install

```bash
go get github.com/sholehbaktiabadi/parallel
```

Requires Go 1.25 or newer (the library uses `sync.WaitGroup.Go`).

## Before and after

Fetching a list of URLs, written by hand:

```go
var (
    mu       sync.Mutex
    wg       sync.WaitGroup
    results  = make([]int, len(urls))
    firstErr error
)

for i, u := range urls {
    wg.Add(1)
    go func() {
        defer wg.Done()
        code, err := fetch(u)
        mu.Lock()
        defer mu.Unlock()
        if err != nil {
            if firstErr == nil {
                firstErr = err
            }
            return
        }
        results[i] = code
    }()
}
wg.Wait()
```

The same thing with this library:

```go
results, err := parallel.Map(urls, fetch)
```

## Run

Use `Run` when you have a few independent jobs and there is nothing to collect.

```go
err := parallel.Run(
    func() error { return sendEmail(user) },
    func() error { return updateCache(user) },
    func() error { return writeAuditLog(user) },
)
```

All three start immediately. `Run` returns once every one of them has finished.

If more than one fails you get **all** of the errors, joined together, and `errors.Is`
still works:

```go
err := parallel.Run(
    func() error { return fmt.Errorf("email: %w", ErrTimeout) },
    func() error { return nil },
    func() error { return ErrDiskFull },
)

errors.Is(err, ErrTimeout)  // true
errors.Is(err, ErrDiskFull) // true
```

## Map

Use `Map` when you run the same job on many items and want the results back.

```go
codes, err := parallel.Map(urls, func(u string) (int, error) {
    resp, err := http.Get(u)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()
    return resp.StatusCode, nil
})
```

Two things worth knowing:

- **The order is preserved.** `codes[3]` is always the result of `urls[3]`, no matter which
  request finished first.
- **You still get the successful results when something fails.** Failed positions hold the
  zero value, and the error tells you what went wrong. One bad URL does not throw away the
  other 99.

## Options

`RunWith` and `MapWith` take an `Options` value:

```go
codes, err := parallel.MapWith(parallel.Options{Limit: 10}, urls, fetch)
```

| Field         | Default | What it does |
|---------------|---------|--------------|
| `Limit`       | `0`     | Maximum number of functions running at the same time. **Zero or less means no limit.** |
| `Context`     | `nil`   | Skip the tasks that have not started yet once the context is cancelled. Only has teeth together with `Limit` — see below. |
| `StopOnError` | `false` | Stop starting new functions as soon as one returns an error. Skipped tasks are reported as `ErrStopped`. Only has teeth together with `Limit` — see below. |

The zero value `Options{}` is exactly what `Run` and `Map` use.

## Errors

Besides your own errors, the package returns three of its own:

| Sentinel        | Means |
|-----------------|-------|
| `ErrPanic`      | One of your functions panicked. The message carries the panic value and a stack trace. |
| `ErrStopped`    | The task never started because `StopOnError` was set and an earlier task had already failed. |
| `ErrIncomplete` | A task ended without returning anything — in practice, it called `runtime.Goexit` (see the gotchas). |

```go
if errors.Is(err, parallel.ErrPanic) {
    // a bug in one of the tasks, not a normal failure - page someone
}
```

A batch stopped by **your** context reports `context.Canceled` / `context.DeadlineExceeded`.
A batch stopped by `StopOnError` reports `ErrStopped`. They are deliberately different, so
"my caller went away" never looks like "one of my tasks failed".

## Gotchas

**Your function must be safe to call from many goroutines at once.** This is the one rule
the library cannot enforce for you. `Map` handles its own results slice safely, but anything
*you* touch inside the function is your responsibility:

```go
// BROKEN: two goroutines appending to the same slice, and writing the same map
var out []int
counts := map[string]int{}
parallel.Map(items, func(it Item) (int, error) {
    out = append(out, it.N)   // data race
    counts[it.Key]++          // data race, and "concurrent map writes" crashes Go
    return it.N, nil
})

// FINE: let Map collect the results for you
out, err := parallel.Map(items, func(it Item) (int, error) {
    return it.N, nil
})
```

Run your tests with `-race`. It catches this immediately.

**There is no limit by default.** `parallel.Map` starts one goroutine per item. With 200
items that is 200 goroutines and 200 simultaneous requests to whatever you are calling.
This is deliberate — a hidden default would be surprising — but it means you should set a
limit whenever the slice can be large:

```go
parallel.MapWith(parallel.Options{Limit: 10}, ids, fetchUser)
```

From [`examples/limit`](examples/limit):

```
== no limit ==
  peak concurrency: 200
== Limit: 10 ==
  peak concurrency: 10
```

**`Context` and `StopOnError` only skip tasks that have not started yet.** Without a `Limit`
nothing waits in a queue — every task starts as fast as the loop can start it — so for a
small batch both options usually arrive with nothing left to skip. If you want a deadline or
fail-fast to actually bite, pair it with a `Limit`:

```go
parallel.MapWith(parallel.Options{Limit: 10, Context: ctx}, ids, fetchUser)
```

**Cancelling does not kill work that is already running.** Nothing in Go can stop a
running function from the outside. If you want a running job to give up too, watch the
context inside your own function:

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

err := parallel.RunWith(parallel.Options{Context: ctx},
    func() error {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // <- ctx goes in here
        if err != nil {
            return err
        }
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return err
        }
        defer resp.Body.Close()
        return nil
    },
)
```

**A panic becomes an error.** If one of your functions panics, the program does not crash.
You get back an error matching `errors.Is(err, parallel.ErrPanic)`, with the panic value and
a stack trace in the message. That is a safety net, not a licence to ignore it — a panic is
still a bug.

**Do not call `t.Fatal` or `require.NoError` inside a task.** The `testing` package forbids
it outside the test's own goroutine: they call `runtime.Goexit`, which `recover` cannot
catch. The library reports such a task as `ErrIncomplete` rather than as a success, but the
right fix is to return the error and assert on it after `Run` returns.

## How it works

The whole trick is that nothing is ever shared between two goroutines.

1. **One slot per task.** Before starting anything, `run` allocates `errs := make([]error, n)`.
   Goroutine number `i` writes only to `errs[i]`, and `MapWith` does the same with
   `results[i]`. Two goroutines never touch the same memory, so there is no mutex anywhere
   in this library.

2. **`wg.Wait()` is the handover.** Nothing reads `errs` or `results` until every goroutine
   has finished. That is what makes step 1 safe, and it is why these functions never return
   while work is still running in the background.

3. **The limit is a channel with N seats.** `slots := make(chan struct{}, Limit)`. Sending
   into it takes a seat and blocks when all seats are taken; the goroutine gives its seat
   back with `<-slots` when it is done. When `Limit` is zero or less the channel is left
   `nil` and skipped entirely.

4. **Cancellation is checked between tasks.** `canStart` checks the context before handing
   out each seat. Because a context never becomes un-cancelled, the first failed check ends
   the loop — the remaining tasks are counted and reported as **one** error, not one error
   each. That matters: reporting them individually turned a cancelled batch of a million
   tasks into a 17 MB error message.

5. **`StopOnError` reuses the same machinery.** A failing task cancels an internal context
   derived from yours. Keeping your context separate is what lets the final error say
   `ErrStopped` instead of pretending you cancelled.

6. **`recover` per task.** Every task runs inside `safeCall`, which uses a deferred
   `recover()` and a named return value to turn a panic into an `ErrPanic`. The error slot is
   pre-filled with `ErrIncomplete` first, so a task that dies without returning at all still
   cannot be mistaken for a success.

7. **`errors.Join`** collapses the error slots into one error, skipping the `nil` ones and
   returning `nil` when everything succeeded.

## Examples

```bash
go run ./examples/basic    # Run and Map, ordering, partial results
go run ./examples/limit    # what Limit actually does, measured
go run ./examples/cancel   # deadlines, StopOnError, cooperative cancellation
```

## When to use something else

Reach for [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup)
instead when you need to hand a context to each task, when you want the group's
cancellation wired into long-running services, or when you want to start tasks dynamically
as earlier ones discover more work. `errgroup` is the more powerful tool; this library is
the one you can explain to a new engineer in five minutes.

## Tests

```bash
go test ./... -race
```

The tests double as documentation — each one demonstrates a single behaviour of the API.
Concurrency is asserted with a barrier rather than a stopwatch, so the suite does not flake
on a loaded machine.

## License

MIT — see [LICENSE](LICENSE).
