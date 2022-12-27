package gokvkit

import (
	"context"
	"testing"
	"time"

	"github.com/autom8ter/machine/v4"
	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
	t.Run("broadcast", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		s := newStream[int](machine.New())
		values := make(chan int, 5)
		go func() {
			ch, err := s.Pull(ctx, "testing")
			assert.NoError(t, err)
			for i := range ch {
				values <- i
			}
			close(values)
		}()
		time.Sleep(1 * time.Second)
		for i := 0; i < 5; i++ {
			s.Broadcast(ctx, "testing", i)
		}
		cancel()
		time.Sleep(1 * time.Second)
		var index = 0
		for i := range values {
			assert.Equal(t, index, i)
			index++
		}
	})
}
