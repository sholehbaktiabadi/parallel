// Example "limit" shows why you usually want Options.Limit when mapping over a
// big slice, and proves that the limit is actually respected.
//
// Run it with:
//
//	go run ./examples/limit
package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sholehbaktiabadi/parallel"
)

func main() {
	ids := make([]int, 200)
	for i := range ids {
		ids[i] = i + 1
	}

	// Without a limit this would start 200 goroutines - and 200 database
	// connections, or 200 HTTP requests at your poor upstream service.
	fmt.Println("== no limit ==")
	report(fetchAll(parallel.Options{}, ids))

	// With Limit: 10 at most ten items are ever in flight.
	fmt.Println("\n== Limit: 10 ==")
	report(fetchAll(parallel.Options{Limit: 10}, ids))
}

// fetchAll pretends to fetch every id and returns how many ran at the same time.
func fetchAll(opt parallel.Options, ids []int) (peak int64, took time.Duration, err error) {
	var inFlight, highest atomic.Int64

	start := time.Now()

	_, err = parallel.MapWith(opt, ids, func(id int) (string, error) {
		now := inFlight.Add(1)
		trackHighest(&highest, now)

		time.Sleep(20 * time.Millisecond) // the "network call"

		inFlight.Add(-1)
		return fmt.Sprintf("user-%d", id), nil
	})

	return highest.Load(), time.Since(start), err
}

func report(peak int64, took time.Duration, err error) {
	fmt.Printf("  peak concurrency: %d\n", peak)
	fmt.Printf("  took:             %v\n", took.Round(10*time.Millisecond))
	fmt.Printf("  error:            %v\n", err)
}

// trackHighest remembers the largest value it has ever been given.
func trackHighest(highest *atomic.Int64, now int64) {
	for {
		max := highest.Load()
		if now <= max || highest.CompareAndSwap(max, now) {
			return
		}
	}
}
