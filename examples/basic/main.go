// Example "basic" shows the two functions you will use 90% of the time:
// parallel.Run and parallel.Map.
//
// Run it with:
//
//	go run ./examples/basic
package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sholehbaktiabadi/parallel"
)

func main() {
	runExample()
	fmt.Println()
	mapExample()
}

// runExample: several independent jobs, nothing to collect, just "do all of these".
func runExample() {
	fmt.Println("== parallel.Run ==")

	start := time.Now()

	err := parallel.Run(
		func() error { return slowStep("send email", 300*time.Millisecond) },
		func() error { return slowStep("update cache", 200*time.Millisecond) },
		func() error { return slowStep("write audit log", 250*time.Millisecond) },
	)

	fmt.Printf("finished in %v (one after another it would be 750ms)\n",
		time.Since(start).Round(10*time.Millisecond))
	fmt.Printf("error: %v\n", err)
}

// mapExample: one job per item, and you want the results back in order.
func mapExample() {
	fmt.Println("== parallel.Map ==")

	users := []string{"anisa", "budi", "citra", "dewi"}

	greetings, err := parallel.Map(users, func(name string) (string, error) {
		// The slowest item is first, so these finish in reverse order...
		time.Sleep(time.Duration(len(users)-indexOf(users, name)) * 100 * time.Millisecond)

		if name == "budi" {
			return "", errors.New("user budi is blocked")
		}
		return "Hello, " + strings.ToUpper(name[:1]) + name[1:], nil
	})

	// ...but the results still line up with the input.
	for i, name := range users {
		fmt.Printf("  users[%d]=%-6s -> %q\n", i, name, greetings[i])
	}

	// Notice that budi failing did not throw away the other three results.
	fmt.Printf("error: %v\n", err)
}

func slowStep(name string, d time.Duration) error {
	time.Sleep(d)
	fmt.Printf("  done: %s (%v)\n", name, d)
	return nil
}

func indexOf(list []string, want string) int {
	for i, s := range list {
		if s == want {
			return i
		}
	}
	return 0
}
