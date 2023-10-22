![CI workflow](https://github.com/unixander/workerpool/actions/workflows/go.yml/badge.svg)
# Workerpool

This is an implementation of pool of workers, that does not repeat execution of executor function if the previous call has not been completed yet. When the request finishes all registered listeners will get the result of function execution.

# Example of usage

```go

// context to stop worker
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// create pool of workers with executor function
pool := workerpool.NewWorkerPool[int, int](
  func(ctx context.Context, value int) (int, error) {
   return value * value, nil
  },
)

// start pool
go pool.Run(ctx)

// submit task
pool.Submit(ctx, i, func(ctx context.Context, value int, err error) {
    fmt.Println("result =", value)
})

// trigger pool stop
cancel()

// wait for all the workers to treminate gracefully
pool.Join(context.Background())

```
