package workerpool

import (
	"context"
	"sync"
)

const DefaultConcurrency = 8

type Task interface {
	Execute(context.Context) error
}

type TaskFunc func(context.Context) error

func (fn TaskFunc) Execute(ctx context.Context) error {
	return fn(ctx)
}

type Future struct {
	done chan error
}

func (future *Future) Wait() error {
	return <-future.done
}

type Pool struct {
	concurrency int
	tasks       chan poolTask
	wg          sync.WaitGroup
}

type poolTask struct {
	ctx    context.Context
	task   Task
	future *Future
}

func New(concurrency int) *Pool {
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	return &Pool{
		concurrency: concurrency,
		tasks:       make(chan poolTask, concurrency),
	}
}

func (pool *Pool) Go(ctx context.Context) {
	for i := 0; i < pool.concurrency; i++ {
		pool.wg.Go(func() {
			for task := range pool.tasks {
				select {
				case <-ctx.Done():
					task.future.done <- ctx.Err()
				default:
					task.future.done <- task.task.Execute(task.ctx)
				}
			}
		})
	}
}

func (pool *Pool) Submit(ctx context.Context, task Task) (*Future, error) {
	future := &Future{done: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pool.tasks <- poolTask{ctx: ctx, task: task, future: future}:
		return future, nil
	}
}

func (pool *Pool) Close() {
	close(pool.tasks)
	pool.wg.Wait()
}
