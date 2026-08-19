package scheduler

import (
	"context"
	"fmt"
)

// Cooperative runs steps until completion or cancellation. Dictionary trainers
// should expose bounded steps through this helper; an uninterruptible third-
// party builder cannot be made preemptible merely by wrapping it in a goroutine.
func Cooperative(ctx context.Context, step func() (done bool, err error)) error {
	if step == nil {
		return fmt.Errorf("scheduler: nil cooperative step")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		done, err := step()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}
