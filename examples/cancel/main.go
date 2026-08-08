// Example "cancel" shows the two ways to stop early: a context, and StopOnError.
//
// It also shows the one thing people get wrong: cancelling does NOT kill work
// that is already running. If you need that, use the context inside your own
// function too - see cooperativeCancel below.
//
// Run it with:
//
//	go run ./examples/cancel
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sholehbaktiabadi/parallel"
)

func main() {
	deadlineExample()
	fmt.Println()
	stopOnErrorExample()
	fmt.Println()
	cooperativeCancel()
}

// deadlineExample: give the whole batch a time budget.
func deadlineExample() {
	fmt.Println("== Context with a deadline ==")

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var started atomic.Int64

	jobs := make([]func() error, 20)
	for i := range jobs {
		jobs[i] = func() error {
			started.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}

	// Two at a time, so the queue is still long when the deadline hits.
	err := parallel.RunWith(parallel.Options{Limit: 2, Context: ctx}, jobs...)

	fmt.Printf("  jobs started: %d of 20\n", started.Load())
	fmt.Printf("  deadline hit: %v\n", errors.Is(err, context.DeadlineExceeded))
}

// stopOnErrorExample: do not keep spending time once something has gone wrong.
func stopOnErrorExample() {
	fmt.Println("== StopOnError ==")

	errBoom := errors.New("job 0 failed")

	var started atomic.Int64

	jobs := make([]func() error, 20)
	for i := range jobs {
		jobs[i] = func() error {
			started.Add(1)
			if i == 0 {
				return errBoom
			}
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}

	err := parallel.RunWith(parallel.Options{Limit: 1, StopOnError: true}, jobs...)

	fmt.Printf("  jobs started:   %d of 20\n", started.Load())
	fmt.Printf("  original error kept:      %v\n", errors.Is(err, errBoom))
	fmt.Printf("  rest reported as stopped: %v\n", errors.Is(err, parallel.ErrStopped))
	// Not context.Canceled: nobody cancelled us, we gave up on our own.
	fmt.Printf("  looks like caller cancel: %v\n", errors.Is(err, context.Canceled))
	fmt.Printf("  full error: %v\n", err)
}

// cooperativeCancel: how to make work that is ALREADY running stop too.
// The library cannot do this for you - only your own function can.
func cooperativeCancel() {
	fmt.Println("== Cancelling work that already started ==")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()

	err := parallel.RunWith(parallel.Options{Context: ctx},
		func() error {
			// This one watches the context itself, so it gives up at 100ms
			// instead of running the full second.
			select {
			case <-time.After(1 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)

	fmt.Printf("  returned after: %v (the job asked for 1s)\n", time.Since(start).Round(10*time.Millisecond))
	fmt.Printf("  error:          %v\n", err)
}
