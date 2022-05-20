package processors

import (
	"context"
	"time"

	api "github.com/kentik/ktranslate/pkg/processors/api/v1"
	"google.golang.org/grpc"
)

type Client struct {
	api.ProcessorClient
	conn *grpc.ClientConn
}

func NewClient(addr string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	// TODO: support more options (i.e. tls)
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	c, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		api.NewProcessorClient(c),
		c,
	}, nil
}
