package main

import (
	"flag"
	"os"
	"strings"

	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"github.com/voi-oss/jaeger-spanner/internal/pkg/store"
	googleGRPC "google.golang.org/grpc"
)

const pluginName = "spanner"

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:       pluginName,
		Level:      hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
		JSONFormat: true,
	})

	var configPath string

	flag.StringVar(&configPath, "config", "", "The absolute path to the Spanner plugin's configuration file")
	flag.Parse()

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	if configPath != "" {
		v.SetConfigFile(configPath)

		err := v.ReadInConfig()
		if err != nil {
			logger.Error("failed to parse configuration file", "error", err)
			os.Exit(1)
		}
	}

	conf := &store.Configuration{}
	conf.InitFromViper(v)

	if conf.ProjectID == "" {
		logger.Error("A project ID for bigTable is required to start this process")
		os.Exit(1)
	}
	if conf.Instance == "" {
		logger.Error("An instance for bigTable is required to start this process")
		os.Exit(1)
	}

	logger.Error("starting Spanner storage", "project", conf.ProjectID, "instance", conf.Instance)

	storePlugin, closeStore, err := store.New(conf, logger)
	if err != nil {
		logger.Error("failed to open store", "error", err)
		os.Exit(1)
	}

	// Self-tracing setup
	cfg := jaegercfg.Configuration{
		ServiceName: pluginName,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		RPCMetrics: true,
	}

	tracer, tracerCloser, err := cfg.NewTracer()
	if err != nil {
		logger.Error("failed to setup tracer", "error", err)
		os.Exit(1)
	}
	defer func() {
		_ = tracerCloser.Close()
	}()

	opentracing.SetGlobalTracer(tracer)

	// gRPC start!
	grpc.ServeWithGRPCServer(&shared.PluginServices{
		Store: storePlugin,
	}, func(options []googleGRPC.ServerOption) *googleGRPC.Server {
		return plugin.DefaultGRPCServer([]googleGRPC.ServerOption{
			googleGRPC.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer)),
			googleGRPC.StreamInterceptor(otgrpc.OpenTracingStreamServerInterceptor(tracer)),
		})
	})

	if err = closeStore(); err != nil {
		logger.Error("failed to close store", "error", err)
	}
}
