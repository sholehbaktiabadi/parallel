# parallel

Run Go functions at the same time, without touching goroutines, channels, `WaitGroup` or mutexes.

Two functions cover almost everything:

```go
parallel.Run(fn1, fn2, fn3)  // run these together, wait for all of them
parallel.Map(items, fn)      // run fn on every item together, collect the results
```

When you need control — a concurrency limit, cancellation, fail-fast — the same two
functions take an `Options` and hand your function a `context.Context`:

```go
parallel.RunWith(opt, fn1, fn2)
parallel.MapWith(opt, items, fn)
```

Four functions, one struct, three sentinel errors. The entire implementation is a single
file ([`parallel.go`](parallel.go)) written to be read — if you have never written a
goroutine before, reading it is the point.

## Install

```bash
go get github.com/sholehbaktiabadi/parallel
```

Requires Go 1.22 or newer.

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

## Options, and the context your function is given

`RunWith` and `MapWith` take an `Options` value, and pass a `context.Context` into your
function:

```go
codes, err := parallel.MapWith(
    parallel.Options{Limit: 10, Context: ctx, StopOnError: true},
    urls,
    func(ctx context.Context, u string) (int, error) {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
        if err != nil {
            return 0, err
        }
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return 0, err
        }
        defer resp.Body.Close()
        return resp.StatusCode, nil
    },
)
```

| Field         | Default | What it does |
|---------------|---------|--------------|
| `Limit`       | `0`     | Maximum number of functions running at the same time. **Zero or less means no limit.** |
| `Context`     | `nil`   | Cancels the run. Tasks that have not started are skipped; running tasks get a cancelled context. |
| `StopOnError` | `false` | Stop as soon as one function fails. Skipped tasks are reported as `ErrStopped`, and running tasks get a cancelled context. |

That context is the whole point of `RunWith`/`MapWith`. Go cannot kill a running function
from the outside — but if you pass the context you are given into whatever you call, your
work stops itself. `Run` and `Map` keep the simpler signature for when you do not need any
of this.

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

## Replacing errgroup

`RunWith` / `MapWith` with `StopOnError: true` is what `errgroup.WithContext` gives you:
a bounded pool, first-failure cancellation, and a context that reaches the running work.

**Fan out over a slice, keeping the results in order.** With errgroup you allocate the
result slice yourself and index into it:

```go
result := make([]Out, len(items))
g, gctx := errgroup.WithContext(ctx)
g.SetLimit(8)
for i, it := range items {
    g.Go(func() error {
        v, err := fetch(gctx, it)
        if err != nil {
            return err
        }
        result[i] = v
        return nil
    })
}
if err := g.Wait(); err != nil {
    return nil, err
}
```

With `MapWith` the slice and the indexing disappear:

```go
result, err := parallel.MapWith(
    parallel.Options{Limit: 8, Context: ctx, StopOnError: true},
    items,
    func(ctx context.Context, it In) (Out, error) { return fetch(ctx, it) },
)
if err != nil {
    return nil, err
}
```

**Fan out into a map.** This is where errgroup usually forces a mutex:

```go
var mu sync.Mutex
grouped := make(map[string][]Article)
g := new(errgroup.Group)
for source, url := range links {
    g.Go(func() error {
        articles, err := fetch(url)
        if err != nil {
            return err
        }
        mu.Lock()
        grouped[source] = articles
        mu.Unlock()
        return nil
    })
}
if err := g.Wait(); err != nil {
    return nil, err
}
```

Map over the keys instead and **the mutex is gone**, because each goroutine only ever
writes to its own index:

```go
sources := make([]string, 0, len(links))
for s := range links {
    sources = append(sources, s)
}

fetched, err := parallel.Map(sources, func(s string) ([]Article, error) {
    return fetch(links[s])
})
if err != nil {
    return nil, err
}
grouped := make(map[string][]Article, len(sources))
for i, s := range sources {
    grouped[s] = fetched[i]
}
```

**What errgroup still does that this does not:** submit work dynamically. `g.Go` can be
called at any time, including from inside another task as it discovers more work. This
library takes a fixed list of functions or a fixed slice up front. If you need a growing
work queue, use errgroup.

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

**`Run` and `Map` cannot stop work that has already started.** They give your function no
context, so a cancelled batch can only skip tasks that have not begun. If you need running
work to wind down, that is exactly what `RunWith` / `MapWith` are for — use the context
they hand you.

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
   derived from yours — and that derived context is the one your tasks were handed, which is
   how running work finds out. Keeping your context separate is what lets the final error
   say `ErrStopped` instead of pretending you cancelled.

6. **`recover` per task.** Every task runs inside `safeCall`, which uses a deferred
   `recover()` and a named return value to turn a panic into an `ErrPanic`. The error slot is
   pre-filled with `ErrIncomplete` first, so a task that dies without returning at all still
   cannot be mistaken for a success.

7. **`errors.Join`** collapses the error slots into one error, skipping the `nil` ones and
   returning `nil` when everything succeeded.

One deliberate non-simplification: the goroutines are started with `wg.Add(1)` and
`defer wg.Done()` rather than `wg.Go`. `wg.Go` is nicer, but it arrived in Go 1.25, and
keeping the floor at 1.22 is worth more than the two saved lines.

## Examples

```bash
go run ./examples/basic    # Run and Map, ordering, partial results
go run ./examples/limit    # what Limit actually does, measured
go run ./examples/cancel   # deadlines, StopOnError, stopping work already running
```

## Tests

```bash
go test ./... -race
```

The tests double as documentation — each one demonstrates a single behaviour of the API.
Concurrency is asserted with a barrier rather than a stopwatch, so the suite does not flake
on a loaded machine.

## License

MIT — see [LICENSE](LICENSE).
