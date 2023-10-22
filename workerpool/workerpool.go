package workerpool

import (
	"container/list"
	"context"
	"log"
	"sync"
	"time"
)

const (
	DEFAULT_MAX_WORKERS = 10
)

// Listener is a function to notify submitters about the results of the executed function
type Listener[R any] func(ctx context.Context, value R, err error)

// Executor is a function to be executed in each worker
type Executor[T comparable, R any] func(ctx context.Context, value T) (R, error)

// Request holds the requests params to be executed
type Request[T comparable] struct {
	Value T
}

// Pool worker is worker with some caching logic.
// If submitted job is already processing there would be no new jobs submitted with the same parameters.
// Job will be added to the queue of listeners and will be waiting for the results from the executor.
type WorkerPool[T comparable, R any] struct {
	mu                     sync.Mutex
	listeners              map[T][]Listener[R] // Mapping of listeners for the specific arguments
	waitingQueue           *list.List          // queue for requests, that were not fitted into the number of active workers
	submitChan             chan *Request[T]    // channel to accept external requests
	innerChan              chan *Request[T]    // channel for submitting tasks to workers
	backSignalChan         chan struct{}       // notifies pool about termination of the worker
	maxWorkers             int                 // maximum number of active workers
	executor               Executor[T, R]      // executes logic to get a result
	joinChan               chan struct{}       // join channel for waiting for all the workers to finish
	maxConcurrentNotifiers int                 // maximum number of concurrent notifiers, that will send results to listeners, for each worker
	startOnce              sync.Once
	workerIdleTimeout      time.Duration // timeout for killing worker after period of being idle
	workersCount           int           // current number of active workers
}

type PoolOption[T comparable, R any] func(pool *WorkerPool[T, R])

// Sets timeout for killing idle workers
func WithWorkerIdleTimeout[T comparable, R any](timeout time.Duration) PoolOption[T, R] {
	return func(pool *WorkerPool[T, R]) {
		pool.workerIdleTimeout = timeout
	}
}

// Sets maximum number of active workers
func WithMaxWorkers[T comparable, R any](maxWorkers int) PoolOption[T, R] {
	return func(pool *WorkerPool[T, R]) {
		pool.maxWorkers = maxWorkers
	}
}

// Sets number of goroutines to concurrently notify listeners
func WithMaxConcurrentNotifiers[T comparable, R any](maxConcurrentNotifiers int) PoolOption[T, R] {
	return func(pool *WorkerPool[T, R]) {
		pool.maxConcurrentNotifiers = maxConcurrentNotifiers
	}
}

func NewWorkerPool[T comparable, R any](executor Executor[T, R], opts ...PoolOption[T, R]) *WorkerPool[T, R] {
	pool := &WorkerPool[T, R]{
		listeners:              make(map[T][]Listener[R]),
		waitingQueue:           list.New(),
		submitChan:             make(chan *Request[T]),
		innerChan:              make(chan *Request[T]),
		backSignalChan:         make(chan struct{}),
		maxWorkers:             DEFAULT_MAX_WORKERS,
		executor:               executor,
		maxConcurrentNotifiers: 10,
		joinChan:               make(chan struct{}),
		workerIdleTimeout:      time.Second * 20,
	}
	for _, opt := range opts {
		opt(pool)
	}
	return pool
}

// Submit sends requested value to execution queue or just adds listeners for already running executors.
func (pool *WorkerPool[T, R]) Submit(ctx context.Context, value T, listeners ...Listener[R]) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if existing, found := pool.listeners[value]; found {
		pool.listeners[value] = append(existing, listeners...)
		return
	} else {
		listenersList := make([]Listener[R], 0, len(listeners))
		listenersList = append(listenersList, listeners...)
		pool.listeners[value] = listenersList
	}

	request := &Request[T]{
		Value: value,
	}

	select {
	case <-ctx.Done():
	case pool.submitChan <- request:
	}
}

func (pool *WorkerPool[T, R]) Run(ctx context.Context) {
	pool.startOnce.Do(func() {
		pool.run(ctx)
	})
}

func (pool *WorkerPool[T, R]) processWaitingQueue(ctx context.Context, wg *sync.WaitGroup) {
	for pool.waitingQueue.Len() > 0 {
		frontElem := pool.waitingQueue.Front()
		waitingRequest := (frontElem.Value).(*Request[T])
		select {
		case <-ctx.Done():
			return
		case value, ok := <-pool.submitChan:
			// New submissions go to the end of waiting queue
			if !ok {
				return
			}
			pool.waitingQueue.PushBack(value)
		case pool.innerChan <- waitingRequest:
			// request is successfully sent to worker, remove it from waiting queue.
			pool.waitingQueue.Remove(frontElem)
		case <-pool.backSignalChan:
			// one worker died, remove it
			if pool.workersCount <= pool.maxWorkers {
				wg.Add(1)
				go pool.worker(ctx, wg, nil)
			} else {
				pool.workersCount--
			}
		}
	}
}

func (pool *WorkerPool[T, R]) run(ctx context.Context) {
	var wg sync.WaitGroup
	killWorkerTimer := time.NewTimer(pool.workerIdleTimeout)
	defer killWorkerTimer.Stop()
	var idle bool

MainLoop:
	for {
		// Processing waitingQueue with the highest priority.
		// new jobs are submitted to the end of waitingQueue
		pool.processWaitingQueue(ctx, &wg)

		// Normal execution, when waitingQueue is empty and there are enough workers
		select {
		case <-ctx.Done():
			break MainLoop
		case incomingRequest, ok := <-pool.submitChan:
			if !ok {
				break MainLoop
			}
			// Take new submission from channel and send to workers
			// if number of workers is not enough - create new worker
			select {
			case pool.innerChan <- incomingRequest:
			default:
				if pool.workersCount < pool.maxWorkers {
					wg.Add(1)
					go pool.worker(ctx, &wg, incomingRequest)
					pool.workersCount++
				} else {
					pool.waitingQueue.PushBack(incomingRequest)
				}
			}
			idle = false
		case <-pool.backSignalChan:
			// Worker died
			pool.workersCount--
		case <-killWorkerTimer.C:
			// if there no incoming jobs, timeout expired and there are no incoming jobs for some time
			// try to shutdown idle workers gradually until the alwaysAliveWorkersCount limit is reached.
			if idle && pool.maxWorkers > 0 {
				select {
				case pool.innerChan <- nil:
					pool.workersCount--
				default:
				}
			}
			// set flag to kill one idle worker on the next timeout
			idle = true
			killWorkerTimer.Reset(pool.workerIdleTimeout)
		}
	}
	wg.Wait()
	close(pool.joinChan)
}

// Join waits for all the workers to finish
func (pool *WorkerPool[T, R]) Join(ctx context.Context) {
	select {
	case <-pool.joinChan:
	case <-ctx.Done():
	}
}

// worker is a unit for execution of incoming requests
func (pool *WorkerPool[T, R]) worker(ctx context.Context, wg *sync.WaitGroup, request *Request[T]) {
	defer func() {
		wg.Done()
		select {
		case <-ctx.Done():
		case pool.backSignalChan <- struct{}{}:
		}
	}()
	defer func() {
		if critical := recover(); critical != nil {
			log.Printf("worker terminated: %v\n", critical)
		}
	}()

	if request != nil {
		pool.poolExecutor(ctx, request)
	}

	for {
		select {
		case request := <-pool.innerChan:
			if request == nil {
				return
			}
			pool.poolExecutor(ctx, request)
		case <-ctx.Done():
			return
		}
	}
}

// poolExecutor runs executor function with the specified value as a parameter
func (pool *WorkerPool[T, R]) poolExecutor(ctx context.Context, request *Request[T]) {
	var result R
	var err error

	result, err = pool.executor(ctx, request.Value)

	// Take all registered listeners from the waiting list
	listeners := func() []Listener[R] {
		pool.mu.Lock()
		defer pool.mu.Unlock()

		listeners, found := pool.listeners[request.Value]
		if !found {
			return nil
		}
		delete(pool.listeners, request.Value)
		return listeners
	}()

	if len(listeners) == 0 {
		return
	}

	// Notify listeners about the results with factor of concurrently running notifiers equals to maxConcurrentNotifiers
	var wg sync.WaitGroup
	concurrentWorkers := make(chan struct{}, pool.maxConcurrentNotifiers)
Loop:
	for _, listener := range listeners {
		select {
		case <-ctx.Done():
			break Loop
		case concurrentWorkers <- struct{}{}:
			wg.Add(1)
			go func(innerCtx context.Context, listener Listener[R], res R, err error) {
				defer func() {
					wg.Done()
					select {
					case <-ctx.Done():
					case <-concurrentWorkers:
					}
				}()
				defer func() {
					if critical := recover(); critical != nil {
						log.Printf("listener terminated: %v\n", critical)
					}
				}()
				listener(innerCtx, res, err)
			}(ctx, listener, result, err)
		}
	}
	wg.Wait()
}
