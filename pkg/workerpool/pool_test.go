package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPoolStartsWorkersWithGo(t *testing.T) {
	ctx := context.Background()
	pool := New(1)
	defer pool.Close()

	future, err := pool.Submit(ctx, TaskFunc(func(context.Context) error {
		return nil
	}))
	require.NoError(t, err)

	select {
	case err := <-future.done:
		require.NoError(t, err)
		require.FailNow(t, "task ran before pool.Go")
	case <-time.After(10 * time.Millisecond):
	}

	pool.Go(ctx)
	require.NoError(t, future.Wait())
}

func TestPoolContextCancellationCompletesFuture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool := New(1)
	pool.Go(ctx)
	defer pool.Close()

	var ran atomic.Bool
	future, err := pool.Submit(context.Background(), TaskFunc(func(context.Context) error {
		ran.Store(true)
		return nil
	}))
	require.NoError(t, err)
	require.ErrorIs(t, future.Wait(), context.Canceled)
	require.False(t, ran.Load())
}

func TestGroupCancelsSiblingsOnFirstError(t *testing.T) {
	ctx := context.Background()
	pool := New(2)
	pool.Go(ctx)
	defer pool.Close()

	group := pool.NewGroup(ctx, 0)
	want := errors.New("task failed")
	firstCanReturn := make(chan struct{})
	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})

	require.NoError(t, group.Submit(TaskFunc(func(context.Context) error {
		<-firstCanReturn
		return want
	})))
	require.NoError(t, group.Submit(TaskFunc(func(ctx context.Context) error {
		close(secondStarted)
		<-ctx.Done()
		close(secondDone)
		return ctx.Err()
	})))

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "second task did not start")
	}

	close(firstCanReturn)
	require.ErrorIs(t, group.Wait(), want)

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		require.FailNow(t, "second task was not cancelled")
	}
}

func TestGroupLimitStopsSubmittingAfterFailedBatch(t *testing.T) {
	ctx := context.Background()
	pool := New(1)
	pool.Go(ctx)
	defer pool.Close()

	group := pool.NewGroup(ctx, 1)
	want := errors.New("task failed")

	require.NoError(t, group.Submit(TaskFunc(func(context.Context) error {
		return want
	})))

	var ran atomic.Bool
	require.ErrorIs(t, group.Submit(TaskFunc(func(context.Context) error {
		ran.Store(true)
		return nil
	})), want)
	require.ErrorIs(t, group.Wait(), want)
	require.False(t, ran.Load())
}
