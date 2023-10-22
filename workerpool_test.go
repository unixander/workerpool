package workerpool_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/unixander/workerpool"
)

func TestWorkerPoolSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lock sync.Mutex
	perValueCalls := make(map[int]int, 100)

	pool := workerpool.NewWorkerPool[int, int](
		func(ctx context.Context, value int) (int, error) {
			lock.Lock()
			defer lock.Unlock()
			perValueCalls[value]++
			return value * value, nil
		},
	)
	go pool.Run(ctx)

	// Stop workers from execution of tasks
	lock.Lock()

	var callbackCalls int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			pool.Submit(ctx, i, func(ctx context.Context, value int, err error) {
				atomic.AddInt32(&callbackCalls, 1)
				wg.Done()
			})
		}
	}

	// Start all of the workers
	lock.Unlock()

	// Wait for finishing all the tasks
	wg.Wait()

	assert.EqualValues(t, 1000, callbackCalls)

	// Each callback should be called exactly once
	for _, numberOfCalls := range perValueCalls {
		assert.Equal(t, 1, numberOfCalls)
	}
}

func TestWorkerPoolWithError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lock sync.Mutex
	perValueCalls := make(map[int]int, 100)

	pool := workerpool.NewWorkerPool[int, int](
		func(ctx context.Context, value int) (int, error) {
			lock.Lock()
			defer lock.Unlock()
			perValueCalls[value]++

			if value%2 == 0 {
				return 0, fmt.Errorf("some error")
			}
			return value * value, nil
		},
	)
	go pool.Run(ctx)

	// Stop workers from execution of tasks
	lock.Lock()

	var callbackCalls int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			pool.Submit(ctx, i, func(ctx context.Context, value int, err error) {
				atomic.AddInt32(&callbackCalls, 1)
				wg.Done()
			})
		}
	}

	// Start all of the workers
	lock.Unlock()

	// Wait for finishing all the tasks
	wg.Wait()

	assert.EqualValues(t, 1000, callbackCalls)

	// Each callback should be called exactly once
	for _, numberOfCalls := range perValueCalls {
		assert.Equal(t, 1, numberOfCalls)
	}
}
