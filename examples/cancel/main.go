// Example "cancel" shows the two ways to stop early: a context, and StopOnError.
//
// The important part is that RunWith and MapWith hand your function a context.
// Tasks that have not started yet are skipped for you; tasks that are already
// running stop themselves by watching the context they were given. That is what
// makes this a drop-in replacement for errgroup.WithContext.
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
	stopRunningWorkExample()
}

// deadlineExample: give the whole batch a time budget.
func deadlineExample() {
	fmt.Println("== Context with a deadline ==")

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var started atomic.Int64

	jobs := make([]func(context.Context) error, 20)
	for i := range jobs {
		jobs[i] = func(context.Context) error {
			started.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}

	// Two at a time, so the queue is still long when the deadline hits.
	err := parallel.RunWith(parallel.Options{Limit: 2, Context: ctx}, jobs...)

	fmt.Printf("  jobs started: %d of 20\n", started.Load())
	fmt.Printf("  deadline hit: %v\n", errors.Is(err, context.DeadlineExceeded))
	fmt.Printf("  error:        %v\n", err)
}

// stopOnErrorExample: do not keep spending time once something has gone wrong.
func stopOnErrorExample() {
	fmt.Println("== StopOnError ==")

	errBoom := errors.New("job 0 failed")

	var started atomic.Int64

	jobs := make([]func(context.Context) error, 20)
	for i := range jobs {
		jobs[i] = func(context.Context) error {
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

// stopRunningWorkExample: the part people get wrong with raw goroutines.
//
// Go cannot kill a running function from the outside. What RunWith can do is hand
// every task a context and cancel it the moment the run gives up. A task that
// passes that context into whatever it calls stops on its own.
func stopRunningWorkExample() {
	fmt.Println("== Stopping work that already started ==")

	errBoom := errors.New("job 1 failed immediately")

	start := time.Now()
	var stoppedEarly atomic.Bool

	err := parallel.RunWith(parallel.Options{StopOnError: true},
		func(ctx context.Context) error {
			// A long job. Without the context it would run for a full second even
			// though the batch has already been abandoned.
			select {
			case <-time.After(1 * time.Second):
				return nil
			case <-ctx.Done():
				stoppedEarly.Store(true)
				return ctx.Err()
			}
		},
		func(ctx context.Context) error {
			return errBoom
		},
	)

	fmt.Printf("  returned after:        %v (the long job asked for 1s)\n", time.Since(start).Round(10*time.Millisecond))
	fmt.Printf("  long job stopped early: %v\n", stoppedEarly.Load())
	fmt.Printf("  original error kept:    %v\n", errors.Is(err, errBoom))
}
