package otel

import (
	"context"
	"os"

	"github.com/kentik/ktranslate/pkg/eggs/logger"
	metricsflow "github.com/kentik/ktranslate/pkg/inputs/otel/processors/metrics/flow"
	tracesflow "github.com/kentik/ktranslate/pkg/inputs/otel/processors/traces/flow"
	"github.com/kentik/ktranslate/pkg/kt"
	"github.com/pkg/errors"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/loggingexporter"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/defaultcomponents"
)

var (
	processorName             = "kentik/flow"
	processorType config.Type = "kentik"
	processorID   config.ComponentID

	defaultCollectorConfig = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  logging:

processors:
  kentik:

extensions:

service:
  telemetry:
    logs:
      level: "debug"
  pipelines:
    traces:
      receivers: [otlp]
      processors: [kentik]
      exporters: [logging]
    metrics:
      receivers: [otlp]
      processors: [kentik]
      exporters: [logging]
`
)

type collector struct {
	configPath  string
	metrics     *OtelListenerMetric
	jchfCh      chan []*kt.JCHF
	processedCh chan int
}
type flowProcessorConfig struct {
	config.ProcessorSettings `mapstructure:",squash"`
}

func newCollector(jchfCh chan []*kt.JCHF, metrics *OtelListenerMetric, log logger.Underlying) (*collector, error) {
	componentID, err := config.NewComponentIDFromString(processorName)
	if err != nil {
		return nil, err
	}
	processorID = componentID

	tmpConfig, err := os.CreateTemp("", "kentik-otel-")
	if err != nil {
		return nil, err
	}
	log.Debugf("creating temp config at %s", tmpConfig.Name())
	if _, err := tmpConfig.Write([]byte(defaultCollectorConfig)); err != nil {
		return nil, err
	}
	tmpConfig.Close()

	return &collector{
		configPath:  tmpConfig.Name(),
		metrics:     metrics,
		jchfCh:      jchfCh,
		processedCh: make(chan int),
	}, nil
}

func (c *collector) start() error {
	defer os.Remove(c.configPath)

	factories, err := components(c.jchfCh, c.processedCh)
	if err != nil {
		return err
	}

	info := component.BuildInfo{
		Command:     "kentik-collector",
		Description: "Kentik OpenTelemetry Collector",
		Version:     "0.1.0",
	}

	go func() {
		for v := range c.processedCh {
			c.metrics.Messages.Mark(int64(v))
		}
	}()

	configPaths := []string{c.configPath}
	cp := service.MustNewDefaultConfigProvider(configPaths, nil)
	app, err := service.New(service.CollectorSettings{BuildInfo: info, Factories: factories, ConfigProvider: cp})
	if err != nil {
		return errors.Wrap(err, "error setting up collector service")
	}

	if err := app.Run(context.Background()); err != nil {
		return errors.Wrap(err, "error starting collector service")
	}

	return nil
}

func components(jchfCh chan []*kt.JCHF, processedCh chan int) (component.Factories, error) {
	factories, err := defaultcomponents.Components()
	if err != nil {
		return component.Factories{}, err
	}

	processors := []component.ProcessorFactory{
		newProcessorFactory(jchfCh, processedCh),
	}
	for _, pr := range factories.Processors {
		processors = append(processors, pr)
	}
	factories.Processors, err = component.MakeProcessorFactoryMap(processors...)
	if err != nil {
		return component.Factories{}, err
	}

	factories.Exporters, err = component.MakeExporterFactoryMap(
		loggingexporter.NewFactory(),
	)
	if err != nil {
		return component.Factories{}, err
	}

	return factories, nil
}

func newProcessorFactory(jchfCh chan []*kt.JCHF, processedCh chan int) component.ProcessorFactory {
	return processorhelper.NewFactory(
		processorType,
		createDefaultConfig,
		component.WithTracesProcessor(tracesflow.CreateTraceProcessor(processedCh)),
		component.WithMetricsProcessor(metricsflow.CreateMetricsProcessor(jchfCh, processedCh)),
	)
}

func createDefaultConfig() config.Processor {
	settings := config.NewProcessorSettings(processorID)
	return &flowProcessorConfig{
		ProcessorSettings: settings,
	}
}
