package parallel_test

import (
	"context"
	"errors"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sholehbaktiabadi/parallel"
)

// barrier returns a task body that blocks until n tasks have all reached it.
// It can only finish if the library really runs n tasks at the same time, so it
// proves concurrency instead of measuring the clock and hoping the machine is fast.
//
// If the tasks do not run together the barrier times out and the test fails with
// a clear message rather than hanging.
func barrier(n int) func() error {
	arrived := make(chan struct{}, n)
	release := make(chan struct{})

	go func() {
		for range n {
			<-arrived
		}
		close(release) // all n are inside the barrier at the same time
	}()

	return func() error {
		arrived <- struct{}{}
		select {
		case <-release:
			return nil
		case <-time.After(5 * time.Second):
			// Real concurrency is established in microseconds, so a generous but
			// short budget is enough. It keeps a genuine regression failing in
			// seconds instead of blowing the whole package timeout.
			return errors.New("timed out in the barrier: the tasks did not run at the same time")
		}
	}
}

var skippedRE = regexp.MustCompile(`(\d+) task\(s\) never started`)

// skippedCount reads back the number of tasks the library says it never started.
func skippedCount(t *testing.T, err error) int {
	t.Helper()

	m := skippedRE.FindStringSubmatch(err.Error())
	if m == nil {
		t.Fatalf("error does not report how many tasks were skipped: %v", err)
	}
	n, convErr := strconv.Atoi(m[1])
	if convErr != nil {
		t.Fatalf("unreadable skipped count %q: %v", m[1], convErr)
	}
	return n
}

func TestRun_AllSucceed(t *testing.T) {
	var ran atomic.Int64

	err := parallel.Run(
		func() error { ran.Add(1); return nil },
		func() error { ran.Add(1); return nil },
		func() error { ran.Add(1); return nil },
	)

	if err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
	if got := ran.Load(); got != 3 {
		t.Errorf("ran %d functions, want 3", got)
	}
}

func TestRun_JoinsAllErrors(t *testing.T) {
	errFirst := errors.New("first failed")
	errThird := errors.New("third failed")

	var ran atomic.Int64

	err := parallel.Run(
		func() error { ran.Add(1); return errFirst },
		func() error { ran.Add(1); return nil },
		func() error { ran.Add(1); return errThird },
	)

	if err == nil {
		t.Fatal("Run() = nil, want an error")
	}
	// Both failures must survive the join - nothing is swallowed.
	if !errors.Is(err, errFirst) {
		t.Errorf("errors.Is(err, errFirst) = false, err = %v", err)
	}
	if !errors.Is(err, errThird) {
		t.Errorf("errors.Is(err, errThird) = false, err = %v", err)
	}
	// The default behaviour is to let every function run, even after a failure.
	if got := ran.Load(); got != 3 {
		t.Errorf("ran %d functions, want 3 (a failure must not skip the others)", got)
	}
}

// brokenLookupTable stands in for the very common bug of forgetting to make() a map.
// Writing to the result panics.
func brokenLookupTable() map[string]int { return nil }

func TestRun_RecoversPanic(t *testing.T) {
	var ranOther atomic.Bool

	// If the panic were not recovered, this test binary would simply crash.
	err := parallel.Run(
		func() error {
			m := brokenLookupTable()
			m["boom"] = 1 // panic: assignment to entry in nil map
			return nil
		},
		func() error { ranOther.Store(true); return nil },
	)

	if err == nil {
		t.Fatal("Run() = nil, want the panic reported as an error")
	}
	if !errors.Is(err, parallel.ErrPanic) {
		t.Errorf("errors.Is(err, ErrPanic) = false, err = %v", err)
	}
	if !strings.Contains(err.Error(), "nil map") {
		t.Errorf("error %q does not carry the original panic message", err)
	}
	// The README promises a stack trace, so the stack trace is part of the contract.
	if !strings.Contains(err.Error(), "parallel_test.go") {
		t.Errorf("error does not contain a stack trace pointing at the failing code:\n%v", err)
	}
	if !ranOther.Load() {
		t.Error("the other function did not run; a panic must not cancel healthy work")
	}
}

func TestRun_ReportsGoexitAsError(t *testing.T) {
	// runtime.Goexit is what t.Fatal and testify's require.* do underneath.
	// recover() cannot catch it, so without a placeholder error this task would
	// be reported as a success and Map would hand back a zero value with err == nil.
	err := parallel.Run(
		func() error {
			runtime.Goexit()
			return nil
		},
		func() error { return nil },
	)

	if !errors.Is(err, parallel.ErrIncomplete) {
		t.Errorf("errors.Is(err, ErrIncomplete) = false, err = %v", err)
	}
}

func TestMap_PreservesOrder(t *testing.T) {
	items := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Early items are the slowest, so the completion order is the reverse of the
	// input order. The results must still line up with the input.
	results, err := parallel.Map(items, func(n int) (string, error) {
		time.Sleep(time.Duration(10-n) * 5 * time.Millisecond)
		return strings.Repeat("x", n), nil
	})

	if err != nil {
		t.Fatalf("Map() error = %v, want nil", err)
	}
	for i, want := range items {
		if got := results[i]; got != strings.Repeat("x", want) {
			t.Errorf("results[%d] = %q, want %q", i, got, strings.Repeat("x", want))
		}
	}
}

func TestMap_ReturnsPartialResults(t *testing.T) {
	errBad := errors.New("2 is not allowed")

	results, err := parallel.Map([]int{1, 2, 3}, func(n int) (int, error) {
		if n == 2 {
			// -1 is returned alongside the error. It must NOT reach the results
			// slice: a failed position is documented to hold the zero value.
			return -1, errBad
		}
		return n * 10, nil
	})

	if !errors.Is(err, errBad) {
		t.Fatalf("Map() error = %v, want errBad", err)
	}
	if results[1] == -1 {
		t.Error("the value returned alongside the error leaked into results")
	}
	want := []int{10, 0, 30}
	for i := range want {
		if results[i] != want[i] {
			t.Errorf("results = %v, want %v", results, want)
			break
		}
	}
}

func TestMapWith_RespectsLimit(t *testing.T) {
	const limit = 3

	var inFlight, peak atomic.Int64

	items := make([]int, 30)

	_, err := parallel.MapWith(parallel.Options{Limit: limit}, items, func(int) (int, error) {
		now := inFlight.Add(1)
		for { // remember the highest concurrency we ever observed
			max := peak.Load()
			if now <= max || peak.CompareAndSwap(max, now) {
				break
			}
		}

		time.Sleep(10 * time.Millisecond)
		inFlight.Add(-1)
		return 0, nil
	})

	if err != nil {
		t.Fatalf("MapWith() error = %v, want nil", err)
	}
	if got := peak.Load(); got > limit {
		t.Errorf("peak concurrency = %d, want at most %d", got, limit)
	}
	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrency = %d, work did not actually run in parallel", got)
	}
}

func TestMapWith_LimitZeroIsUnlimited(t *testing.T) {
	const n = 50

	// Limit 0 is documented as "no limit". If a hidden default throttle were ever
	// introduced, fewer than n tasks would run together and the barrier would time out.
	block := barrier(n)

	_, err := parallel.MapWith(parallel.Options{Limit: 0}, make([]int, n), func(int) (int, error) {
		return 0, block()
	})

	if err != nil {
		t.Fatalf("MapWith() error = %v; %d tasks did not all run at the same time", err, n)
	}
}

func TestRunWith_NegativeLimit(t *testing.T) {
	const n = 20

	// A negative Limit is documented the same as zero: no limit.
	block := barrier(n)

	fns := make([]func() error, n)
	for i := range fns {
		fns[i] = block
	}

	if err := parallel.RunWith(parallel.Options{Limit: -5}, fns...); err != nil {
		t.Fatalf("RunWith(Limit: -5) error = %v, want nil (negative means no limit)", err)
	}
}

func TestRunWith_StopOnError(t *testing.T) {
	errBoom := errors.New("boom")

	var ran atomic.Int64

	fns := make([]func() error, 50)
	for i := range fns {
		fns[i] = func() error {
			ran.Add(1)
			if i == 0 {
				return errBoom
			}
			time.Sleep(5 * time.Millisecond)
			return nil
		}
	}

	// Limit: 1 makes the functions start one after another, so the failure of the
	// first one lands before the rest have a chance to begin.
	err := parallel.RunWith(parallel.Options{Limit: 1, StopOnError: true}, fns...)

	if !errors.Is(err, errBoom) {
		t.Fatalf("RunWith() error = %v, want errBoom", err)
	}
	// Functions that never started are reported as ErrStopped...
	if !errors.Is(err, parallel.ErrStopped) {
		t.Errorf("errors.Is(err, ErrStopped) = false, err = %v", err)
	}
	// ...and NOT as context.Canceled: the caller never passed a context, so
	// claiming cancellation would hide the difference between "my caller went
	// away" and "one of my own tasks failed".
	if errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = true although the caller passed no context: %v", err)
	}
	// At most one more function can slip through while the cancellation propagates.
	if got := ran.Load(); got > 2 {
		t.Errorf("ran %d functions after the first one failed, want at most 2", got)
	}
	// Every function is either run or counted as skipped - never silently dropped.
	// This invariant holds however the cancellation race resolves, so it catches an
	// off-by-one in the count without being flaky.
	if got := int(ran.Load()) + skippedCount(t, err); got != len(fns) {
		t.Errorf("ran + skipped = %d, want %d: some functions were neither run nor counted", got, len(fns))
	}
}

func TestRunWith_LateCallerCancelDoesNotStealTheBlame(t *testing.T) {
	errBoom := errors.New("permanent failure")

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	fns := make([]func() error, 10)
	for i := range fns {
		fns[i] = func() error {
			if i == 1 {
				return errBoom // fails at once, long before the deadline
			}
			// Healthy but slow: this keeps the run blocked well past the caller's
			// deadline, so the deadline expires while we are draining.
			time.Sleep(600 * time.Millisecond)
			return nil
		}
	}

	err := parallel.RunWith(parallel.Options{Limit: 2, Context: ctx, StopOnError: true}, fns...)

	if !errors.Is(err, errBoom) {
		t.Fatalf("RunWith() error = %v, want errBoom", err)
	}
	// StopOnError skipped the remaining tasks almost immediately. The caller's
	// deadline expired 150ms later and skipped nothing, so it must not take the
	// blame just because it happened to fire before the run finished draining.
	if !errors.Is(err, parallel.ErrStopped) {
		t.Errorf("errors.Is(err, ErrStopped) = false; the skip was blamed on the caller instead:\n%v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = true, but the deadline skipped no tasks:\n%v", err)
	}
}

func TestRunWith_GoexitTriggersStopOnError(t *testing.T) {
	var ran atomic.Int64

	fns := make([]func() error, 50)
	for i := range fns {
		fns[i] = func() error {
			ran.Add(1)
			if i == 0 {
				runtime.Goexit() // never returns, so the end of this func is unreachable
			}
			time.Sleep(5 * time.Millisecond)
			return nil
		}
	}

	err := parallel.RunWith(parallel.Options{Limit: 1, StopOnError: true}, fns...)

	// The run reports the task as failed...
	if !errors.Is(err, parallel.ErrIncomplete) {
		t.Fatalf("errors.Is(err, ErrIncomplete) = false, err = %v", err)
	}
	// ...so fail-fast has to treat it as a failure too. A plain statement after
	// safeCall would be skipped by the Goexit unwind and the run would continue.
	if !errors.Is(err, parallel.ErrStopped) {
		t.Errorf("errors.Is(err, ErrStopped) = false: StopOnError ignored an abnormally ended task:\n%v", err)
	}
	if got := ran.Load(); got > 2 {
		t.Errorf("ran %d functions after the first one died, want at most 2", got)
	}
}

func TestRunWith_LimitWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// canStart checks the context BEFORE reaching for a seat. Without that check a
	// free seat and a cancelled context are both ready in the select, Go picks
	// between them at random, and work starts that should never have started.
	// One run proves nothing about a random choice, so repeat it.
	for range 200 {
		var ran atomic.Int64

		fns := make([]func() error, 20)
		for i := range fns {
			fns[i] = func() error { ran.Add(1); return nil }
		}

		err := parallel.RunWith(parallel.Options{Limit: 4, Context: ctx}, fns...)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunWith() error = %v, want context.Canceled", err)
		}
		if got := ran.Load(); got != 0 {
			t.Fatalf("ran %d of 20 functions with an already-cancelled context and Limit 4, want 0", got)
		}
		if got := skippedCount(t, err); got != 20 {
			t.Fatalf("reported %d skipped tasks, want 20", got)
		}
	}
}

func TestRunWith_StopOnErrorWithoutFailures(t *testing.T) {
	var ran atomic.Int64

	fns := make([]func() error, 20)
	for i := range fns {
		fns[i] = func() error { ran.Add(1); return nil }
	}

	// Nothing fails, so StopOnError must stay completely out of the way.
	err := parallel.RunWith(parallel.Options{Limit: 2, StopOnError: true}, fns...)

	if err != nil {
		t.Fatalf("RunWith() error = %v, want nil", err)
	}
	if got := ran.Load(); got != 20 {
		t.Errorf("ran %d of 20 functions; StopOnError cancelled work even though nothing failed", got)
	}
}

func TestRunWith_SkippedTasksReportedOnce(t *testing.T) {
	errBoom := errors.New("boom")

	fns := make([]func() error, 10_000)
	for i := range fns {
		fns[i] = func() error {
			if i == 0 {
				return errBoom
			}
			time.Sleep(time.Millisecond)
			return nil
		}
	}

	err := parallel.RunWith(parallel.Options{Limit: 1, StopOnError: true}, fns...)

	if !errors.Is(err, errBoom) {
		t.Fatalf("RunWith() error = %v, want errBoom", err)
	}
	// ~10,000 tasks are skipped. They must be summarised in one line, not stored
	// one error each - otherwise logging this error ships megabytes of identical text.
	if size := len(err.Error()); size > 500 {
		t.Errorf("error message is %d bytes after skipping ~10,000 tasks; it must not grow with the task count:\n%.400s...", size, err)
	}
}

func TestMapWith_StopOnError(t *testing.T) {
	errBad := errors.New("bad item")

	var ran atomic.Int64

	items := make([]int, 500)

	// MapWith must honour StopOnError, not just Limit.
	_, err := parallel.MapWith(parallel.Options{Limit: 1, StopOnError: true}, items, func(int) (int, error) {
		if ran.Add(1) == 1 {
			return 0, errBad
		}
		time.Sleep(time.Millisecond)
		return 0, nil
	})

	if !errors.Is(err, errBad) {
		t.Fatalf("MapWith() error = %v, want errBad", err)
	}
	if !errors.Is(err, parallel.ErrStopped) {
		t.Errorf("errors.Is(err, ErrStopped) = false, err = %v", err)
	}
	if got := ran.Load(); got > 2 {
		t.Errorf("ran %d items after the first failure, want at most 2", got)
	}
}

func TestMapWith_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var ran atomic.Int64

	// MapWith must honour Context, not just Limit.
	results, err := parallel.MapWith(parallel.Options{Context: ctx}, []int{1, 2, 3}, func(n int) (int, error) {
		ran.Add(1)
		return n, nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("MapWith() error = %v, want context.Canceled", err)
	}
	if got := ran.Load(); got != 0 {
		t.Errorf("ran %d items, want 0 (the context was already cancelled)", got)
	}
	for i, r := range results {
		if r != 0 {
			t.Errorf("results[%d] = %d, want the zero value", i, r)
		}
	}
}

func TestRunWith_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before we even start

	var ran atomic.Int64

	err := parallel.RunWith(parallel.Options{Context: ctx},
		func() error { ran.Add(1); return nil },
		func() error { ran.Add(1); return nil },
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWith() error = %v, want context.Canceled", err)
	}
	// The caller's own cancellation must not be dressed up as ErrStopped.
	if errors.Is(err, parallel.ErrStopped) {
		t.Errorf("errors.Is(err, ErrStopped) = true, but it was the caller who cancelled: %v", err)
	}
	if got := ran.Load(); got != 0 {
		t.Errorf("ran %d functions, want 0 (the context was already cancelled)", got)
	}
	// Both functions were skipped, and the error has to say so exactly.
	if got := skippedCount(t, err); got != 2 {
		t.Errorf("reported %d skipped tasks, want 2", got)
	}
}

func TestRunWith_ContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	var ran atomic.Int64

	fns := make([]func() error, 10)
	for i := range fns {
		fns[i] = func() error {
			ran.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}

	// Only two seats, so the later functions are still queued when the deadline hits.
	err := parallel.RunWith(parallel.Options{Limit: 2, Context: ctx}, fns...)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunWith() error = %v, want context.DeadlineExceeded", err)
	}
	if got := ran.Load(); got >= 10 {
		t.Errorf("ran %d functions, want fewer than 10", got)
	}
}

func TestRunWith_ContextDeadlineBeatsStopOnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	fns := make([]func() error, 10)
	for i := range fns {
		fns[i] = func() error {
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}

	// Both cancellation paths are armed but only the caller's deadline actually
	// fires, so the skipped tasks must blame the deadline - not ErrStopped.
	err := parallel.RunWith(parallel.Options{Limit: 2, Context: ctx, StopOnError: true}, fns...)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunWith() error = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, parallel.ErrStopped) {
		t.Errorf("errors.Is(err, ErrStopped) = true, but no task failed: %v", err)
	}
}

func TestRun_IsActuallyConcurrent(t *testing.T) {
	const n = 10

	// Every task waits until all n have arrived. If Run executed them one after
	// another the first task would wait forever, so this cannot pass by accident
	// on a fast machine or flake on a slow one - unlike a wall-clock budget.
	block := barrier(n)

	fns := make([]func() error, n)
	for i := range fns {
		fns[i] = block
	}

	if err := parallel.Run(fns...); err != nil {
		t.Fatalf("Run() error = %v; the functions did not run at the same time", err)
	}
}

func TestRun_NoFunctions(t *testing.T) {
	if err := parallel.Run(); err != nil {
		t.Errorf("Run() = %v, want nil", err)
	}
}

func TestMap_EmptyInput(t *testing.T) {
	results, err := parallel.Map([]string{}, func(s string) (int, error) {
		return len(s), nil
	})
	if err != nil {
		t.Errorf("Map() error = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Errorf("results = %v, want empty", results)
	}
}

func TestMap_NilSlice(t *testing.T) {
	results, err := parallel.Map(nil, func(s string) (int, error) {
		return len(s), nil
	})
	if err != nil {
		t.Errorf("Map() error = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Errorf("results = %v, want empty", results)
	}
}
