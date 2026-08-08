// Package parallel runs functions at the same time, without you having to touch
// goroutines, channels, WaitGroups or mutexes.
//
// There are only two functions you need to know:
//
//	parallel.Run(fn1, fn2, fn3)  // run these functions together, wait for all of them
//	parallel.Map(items, fn)      // run fn on every item together, collect the results
//
// Both of them wait until everything is finished, and both of them give you back
// a single error that contains every failure that happened.
//
// When you need more control (limit how many run at once, cancel early, stop on the
// first error) use RunWith and MapWith with an Options value.
//
// Your functions must be safe to call from several goroutines at once. That is the
// one rule this library cannot enforce for you - see the README.
//
// The whole library is this one file. It is meant to be read, not just imported.
package parallel

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// The errors this package produces itself. Everything else you get back came out
// of your own functions.
var (
	// ErrPanic marks an error that was built from a panic inside one of your
	// functions. Test for it with errors.Is(err, parallel.ErrPanic).
	ErrPanic = errors.New("parallel: task panicked")

	// ErrStopped marks the tasks that never started because Options.StopOnError
	// was set and an earlier task had already failed.
	//
	// It exists so that you can tell "we gave up early" apart from "my caller
	// cancelled me": the second one shows up as context.Canceled instead.
	ErrStopped = errors.New("parallel: stopped after an earlier task failed")

	// ErrIncomplete marks a task that ended without returning anything at all.
	//
	// In practice this only happens when a task calls runtime.Goexit - most often
	// by calling t.Fatal, t.FailNow or testify's require.* from inside a task,
	// which the testing package does not allow outside the test's own goroutine.
	// Without this error such a task would look like a success.
	ErrIncomplete = errors.New("parallel: task exited without returning a result")
)

// Options controls how the work is executed. The zero value (Options{}) means
// "run everything at once, never cancel, never stop early" - which is exactly
// what Run and Map use.
type Options struct {
	// Limit is the maximum number of functions allowed to run at the same time.
	//
	// Zero or less (the default) means no limit: every function starts immediately.
	// That is fine for a handful of tasks, but if you are mapping over 10,000
	// items you almost certainly want something like Limit: 10.
	Limit int

	// Context lets you stop early. When it is cancelled (or its deadline passes),
	// the tasks that have not started yet are skipped.
	//
	// Two things this does NOT do, both of which surprise people:
	//
	// Functions that are ALREADY running are never killed - Go cannot kill a
	// running function from the outside. If you need to interrupt work in flight,
	// use the context inside your own function too. See the README.
	//
	// Without a Limit there is nothing waiting in a queue, because every task
	// starts as fast as the loop can start it. So for a small batch and Limit 0
	// a cancellation usually arrives with nothing left to skip. Context has real
	// teeth when it is combined with Limit.
	//
	// nil (the default) means "never cancel".
	Context context.Context

	// StopOnError makes the run stop starting new functions as soon as one of
	// them returns an error. Functions that are already running still finish,
	// and the skipped ones are reported as ErrStopped.
	//
	// Like Context, this only affects tasks that have not started yet, so it is
	// only useful together with Limit. With Limit 0 every task has usually
	// started before the first error arrives, and this option does nothing.
	//
	// false (the default) means every function gets a chance to run, and you get
	// all of their errors back together.
	StopOnError bool
}

// Run executes every function at the same time and waits until all of them are done.
//
//	err := parallel.Run(
//	    func() error { return sendEmail() },
//	    func() error { return updateCache() },
//	)
//
// If several functions fail you get all of their errors joined together, and
// errors.Is / errors.As still work on the result.
func Run(fns ...func() error) error {
	return RunWith(Options{}, fns...)
}

// RunWith is Run with control over concurrency, cancellation and early stopping.
//
//	err := parallel.RunWith(parallel.Options{Limit: 2}, fns...)
func RunWith(opt Options, fns ...func() error) error {
	return run(opt, len(fns), func(i int) error {
		return fns[i]()
	})
}

// Map calls fn on every item at the same time and collects the results.
//
//	codes, err := parallel.Map(urls, func(u string) (int, error) {
//	    resp, err := http.Get(u)
//	    if err != nil {
//	        return 0, err
//	    }
//	    defer resp.Body.Close()
//	    return resp.StatusCode, nil
//	})
//
// The results keep the order of the input: results[i] always belongs to items[i],
// no matter which item finished first.
//
// If some items fail you still get the results slice back - the failed positions
// simply hold the zero value. That way one bad item does not throw away the good ones.
//
// Note: Map starts one goroutine per item. For a large slice, use MapWith with a Limit.
func Map[T, R any](items []T, fn func(T) (R, error)) ([]R, error) {
	return MapWith(Options{}, items, fn)
}

// MapWith is Map with control over concurrency, cancellation and early stopping.
//
//	// at most 10 downloads at the same time
//	codes, err := parallel.MapWith(parallel.Options{Limit: 10}, urls, fetch)
func MapWith[T, R any](opt Options, items []T, fn func(T) (R, error)) ([]R, error) {
	results := make([]R, len(items))

	err := run(opt, len(items), func(i int) error {
		r, err := fn(items[i])
		if err != nil {
			return err
		}
		// No mutex needed here. Goroutine number i is the only one that ever
		// touches results[i], so two goroutines can never write to the same place.
		results[i] = r
		return nil
	})

	return results, err
}

// run is the only place in this library where concurrency actually happens.
// It executes n tasks, where task(i) does the i-th piece of work.
func run(opt Options, n int, task func(i int) error) error {
	// One error slot per task. Like results in MapWith, every goroutine owns
	// exactly one index, so no lock is required to fill this in.
	errs := make([]error, n)

	// The caller's context. We keep it separate from ctx below so that when we stop
	// handing out work we can tell "the caller cancelled us" apart from
	// "StopOnError cancelled us".
	parent := opt.Context
	if parent == nil {
		parent = context.Background()
	}

	// stop() is what a failing task calls. Unless StopOnError is set it does nothing,
	// which keeps the loop below free of if-statements.
	ctx := parent
	stop := func() {}
	if opt.StopOnError {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(parent)
		defer cancel() // always release the context, error or not
		stop = cancel
	}

	// slots is a "there are only this many seats" channel, usually called a semaphore.
	// Sending into it takes a seat, receiving from it gives the seat back. Because the
	// channel is buffered with Limit slots, the send blocks once all seats are taken.
	//
	// When Limit is zero or less we leave slots as nil and skip it entirely -
	// nil means "no limit".
	var slots chan struct{}
	if opt.Limit > 0 {
		slots = make(chan struct{}, opt.Limit)
	}

	var wg sync.WaitGroup
	skipped := 0
	var reason error // why we stopped handing out work

	for i := range n {
		// Wait for a free seat, and give up if we get cancelled while waiting.
		// A context never becomes un-cancelled, so once this fails every remaining
		// task is skipped too and there is no reason to keep looping.
		if !canStart(ctx, slots) {
			skipped = n - i

			// Work out the reason HERE, while it is still the true one. Reading it
			// after wg.Wait() would blame the caller for a StopOnError stop whenever
			// their context happened to expire while a slow task was still finishing.
			reason = parent.Err() // did the caller's context stop us...
			if reason == nil {
				reason = ErrStopped // ...or was it StopOnError?
			}
			break
		}

		// wg.Go starts the goroutine and handles the bookkeeping that used to be
		// wg.Add(1) plus defer wg.Done() - one less thing to get wrong.
		wg.Go(func() {
			if slots != nil {
				defer func() { <-slots }() // give the seat back for the next task
			}

			// A defer, not a plain statement at the end: a task killed by
			// runtime.Goexit never reaches the end of this function, and fail-fast
			// still has to fire for it.
			defer func() {
				if errs[i] != nil {
					stop() // does nothing unless StopOnError is set
				}
			}()

			// If the task ends abnormally the assignment below never happens, so
			// we put a placeholder in first. Without it, a task killed by
			// runtime.Goexit (t.Fatal and friends) would be reported as a success.
			errs[i] = ErrIncomplete
			errs[i] = safeCall(task, i)
		})
	}

	// Nothing below this line runs until every goroutine above has finished, so by
	// the time we read errs (and results, in MapWith) all writes are done.
	wg.Wait()

	// errors.Join skips nil entries and returns nil if every entry is nil.
	err := errors.Join(errs...)

	// The tasks that never started are reported once, together - not once each.
	// Cancelling a batch of a million would otherwise hand you back an error whose
	// message is a million identical lines long.
	if skipped > 0 {
		err = errors.Join(err, fmt.Errorf("%w (%d task(s) never started)", reason, skipped))
	}

	return err
}

// canStart waits for a free seat and reports whether the next task may begin.
// It returns false once the context has been cancelled.
func canStart(ctx context.Context, slots chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	if slots == nil {
		return true // no limit, so there is nothing to wait for
	}

	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// safeCall runs one task and turns a panic into an ordinary error.
//
// Without this, a panic in any goroutine takes down the whole program - and the
// stack trace points at our goroutine, not at the code that called us. Here the
// panic becomes just another error you can check with errors.Is(err, ErrPanic).
func safeCall(task func(i int) error, i int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Named return value: assigning to err here changes what safeCall returns.
			err = fmt.Errorf("%w: %v\n\n%s", ErrPanic, r, debug.Stack())
		}
	}()

	return task(i)
}
