package gokvkit

import (
	"context"

	"github.com/autom8ter/machine/v4"
)

type Stream[T any] interface {
	Broadcast(ctx context.Context, channel string, msg T)
	Pull(ctx context.Context, channel string) (<-chan T, error)
}

type defaultStream[T any] struct {
	machine machine.Machine
}

func newStream[T any](m machine.Machine) Stream[T] {
	return defaultStream[T]{machine: m}
}

func (d defaultStream[T]) Broadcast(ctx context.Context, channel string, msg T) {
	d.machine.Publish(ctx, machine.Message{
		Channel: channel,
		Body:    msg,
	})
}

func (d defaultStream[T]) Pull(ctx context.Context, channel string) (<-chan T, error) {
	ch := make(chan T)
	d.machine.Go(ctx, func(ctx context.Context) error {
		err := d.machine.Subscribe(ctx, channel, func(ctx context.Context, msg machine.Message) (bool, error) {
			ch <- msg.Body.(T)
			return true, nil
		})
		close(ch)
		return err
	})
	return ch, nil
}
