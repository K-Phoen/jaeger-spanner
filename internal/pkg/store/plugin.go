package store

import (
	"io"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
)

var (
	_ shared.StoragePlugin = (*Plugin)(nil)
	_ io.Closer            = (*Plugin)(nil)
)

const (
	prefix = "spanner."

	flagProjectID = prefix + "projectID"
	flagInstance  = prefix + "instance"
)

type Plugin struct {
	reader       *Reader
	dependencies *DependenciesReader
	writer       *Writer
}

type Configuration struct {
	ProjectID string `yaml:"projectID"`
	Instance  string `yaml:"instance"`
}

// InitFromViper initializes the options struct with values from Viper
func (c *Configuration) InitFromViper(v *viper.Viper) {
	c.ProjectID = v.GetString(flagProjectID)
	c.Instance = v.GetString(flagInstance)
}

func New(conf *Configuration, logger hclog.Logger) (*Plugin, func() error, error) {
	writer, err := NewWriter(logger, conf.ProjectID, conf.Instance)
	if err != nil {
		return nil, nil, err
	}

	reader, err := NewReader(conf.ProjectID, conf.Instance)
	if err != nil {
		return nil, nil, err
	}

	dependencies, err := NewDependenciesReader()
	if err != nil {
		return nil, nil, err
	}

	store := &Plugin{
		reader:       reader,
		dependencies: dependencies,
		writer:       writer,
	}

	return store, store.Close, nil
}

func (p *Plugin) Close() error {
	// FIXME: it's fine to silence the error for now because Close() never returns an error.
	// We can't assume it will always be true.
	_ = p.reader.Close()

	return p.writer.Close()
}

func (p *Plugin) SpanReader() spanstore.Reader {
	return p.reader
}

func (p *Plugin) SpanWriter() spanstore.Writer {
	return p.writer
}

func (p *Plugin) DependencyReader() dependencystore.Reader {
	return p.dependencies
}
