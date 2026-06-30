package workerpool

import (
	"context"
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
