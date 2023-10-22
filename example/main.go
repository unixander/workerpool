package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unixander/workerpool/workerpool"
)

func concurrentSubmitter(ctx context.Context, pool *workerpool.WorkerPool[int, int], wg *sync.WaitGroup) {
	wg.Add(100)
	for i := 0; i < 100; i++ {
		input := i
		pool.Submit(ctx, input, func(ctx context.Context, value int, err error) {
			fmt.Printf("%d ** 2 = %d\n", input, value)
			wg.Done()
		})
	}
}

// Example of using of workerpool to get square of int values.
// multiple submitters are called,
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lock sync.Mutex
	perValueCalls := make(map[int]int, 100)

	pool := workerpool.NewWorkerPool[int, int](
		func(ctx context.Context, value int) (int, error) {
			time.Sleep(time.Second) // simulate some latency for execution

			lock.Lock()
			defer lock.Unlock()
			perValueCalls[value]++

			return value * value, nil
		},
	)
	go pool.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		go concurrentSubmitter(ctx, pool, &wg)
	}
	// Wait for finishing all the tasks
	wg.Wait()

	// Trigger exit signal
	cancel()
	// Wait for all the workers to terminate gracefully
	pool.Join(context.Background())

	// Print statistics of calling executor
	for value, numberOfCalls := range perValueCalls {
		fmt.Printf("executor(%d) was called %d time(s)\n", value, numberOfCalls)
	}
}
