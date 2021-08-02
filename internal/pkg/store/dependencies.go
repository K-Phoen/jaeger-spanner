package store

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
)

var _ dependencystore.Reader = (*DependenciesReader)(nil)
var _ io.Closer = (*DependenciesReader)(nil)

type DependenciesReader struct {
	client *spanner.Client
}

func NewDependenciesReader() (*DependenciesReader, error) {

	return &DependenciesReader{}, nil
}

func (r *DependenciesReader) GetDependencies(ctx context.Context, endTS time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, nil
}

func (r *DependenciesReader) Close() error {
	r.client.Close()

	return nil
}
