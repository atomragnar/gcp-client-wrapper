package gcw

import (
	"context"
	"errors"

	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

type ApiResponseHandler interface {
	HandleResponse(proto.Message)
}

type ClientInterface interface {
	Close() error
}

type ClientWrapper[T ClientInterface] struct {
	new func() (T, error)
	Ctx context.Context
}

func (c *ClientWrapper[T]) New() (T, error) {
	return c.new()
}

func NewClientWrapper[T ClientInterface](ctx context.Context, new func() (T, error)) ClientWrapper[T] {
	return ClientWrapper[T]{
		new: new,
		Ctx: ctx,
	}
}

type IteratorInterface[T proto.Message] interface {
	PageInfo() *iterator.PageInfo
	Next() (T, error)
}

type RequestFunc[T ClientInterface, V proto.Message, R proto.Message] func(T, R, context.Context) (V, error)

type RequestWrapper[T ClientInterface, V proto.Message, R proto.Message] struct {
	Req         R
	requestFunc RequestFunc[T, V, R]
}

func NewRequest[T ClientInterface, V proto.Message, R proto.Message](req R, fn RequestFunc[T, V, R]) *RequestWrapper[T, V, R] {
	return &RequestWrapper[T, V, R]{
		Req:         req,
		requestFunc: fn,
	}
}

func (rw *RequestWrapper[T, V, R]) MakeRequest(c ClientWrapper[T], h ApiResponseHandler) error {
	client, err := c.New()
	if err != nil {
		return err
	}
	defer func(client T) {
		err := client.Close()
		if err != nil {
		}
	}(client)

	r, err := rw.requestFunc(client, rw.Req, c.Ctx)
	if err != nil {
		return err
	}
	h.HandleResponse(r)
	return nil
}

type CreateIteratorFunc[T ClientInterface, V proto.Message, R proto.Message] func(T, R, context.Context) IteratorInterface[V]

type IteratorRequestWrapper[T ClientInterface, V proto.Message, R proto.Message] struct {
	Req            R
	createIterator CreateIteratorFunc[T, V, R]
}

func NewIteratorRequest[T ClientInterface, V proto.Message, R proto.Message](req R, fn CreateIteratorFunc[T, V, R]) *IteratorRequestWrapper[T, V, R] {
	return &IteratorRequestWrapper[T, V, R]{
		Req:            req,
		createIterator: fn,
	}
}

func (irw *IteratorRequestWrapper[T, V, R]) MakeRequest(c ClientWrapper[T], h ApiResponseHandler) error {
	client, err := c.New()
	if err != nil {
		return err
	}
	defer func(client T) {
		err := client.Close()
		if err != nil {
		}
	}(client)

	it := irw.createIterator(client, irw.Req, c.Ctx)

	for {
		resp, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}
		h.HandleResponse(resp)
	}
	return nil
}
