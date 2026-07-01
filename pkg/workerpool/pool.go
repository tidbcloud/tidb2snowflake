package workerpool

import (
	"context"
	"sync"
)

const DefaultConcurrency = 16

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

// Group submits related tasks and cancels the rest after the first error.
type Group struct {
	pool    *Pool
	ctx     context.Context
	cancel  context.CancelFunc
	limit   int
	futures []*Future
	first   error
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
				case <-task.ctx.Done():
					task.future.done <- task.ctx.Err()
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

func (pool *Pool) NewGroup(ctx context.Context, limit int) *Group {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{
		pool:   pool,
		ctx:    ctx,
		cancel: cancel,
		limit:  limit,
	}
}

func (group *Group) Submit(task Task) error {
	if group.first != nil {
		return group.first
	}
	if group.limit > 0 && len(group.futures) >= group.limit {
		if err := group.drain(); err != nil {
			return err
		}
	}
	future, err := group.pool.Submit(group.ctx, task)
	if err != nil {
		return group.capture(err)
	}
	group.futures = append(group.futures, future)
	return nil
}

func (group *Group) Wait() error {
	defer group.cancel()
	return group.drain()
}

func (group *Group) Cancel() {
	group.cancel()
}

func (group *Group) drain() error {
	for _, future := range group.futures {
		group.capture(future.Wait())
	}
	group.futures = group.futures[:0]
	return group.first
}

func (group *Group) capture(err error) error {
	if err != nil && group.first == nil {
		group.first = err
		group.cancel()
	}
	return group.first
}

func (pool *Pool) Close() {
	close(pool.tasks)
	pool.wg.Wait()
}
