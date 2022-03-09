package flow

import (
	"context"

	"github.com/kentik/ktranslate/pkg/kt"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
)

type metricFlowProcessor struct {
	nextConsumer consumer.Metrics
	logger       *zap.Logger
	jchfCh       chan []*kt.JCHF
	processedCh  chan int
}

var (
	_ component.MetricsProcessor = (*metricFlowProcessor)(nil)
)

func NewMetricFlowProcessor(ch chan []*kt.JCHF, processedCh chan int, l *zap.Logger, cfg config.Processor, nextConsumer consumer.Metrics) (*metricFlowProcessor, error) {
	logger := l.With(zap.String("processor", "metricFlow"))
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	return &metricFlowProcessor{
		logger:       logger,
		nextConsumer: nextConsumer,
		jchfCh:       ch,
		processedCh:  processedCh,
	}, nil
}

func (p *metricFlowProcessor) ConsumeMetrics(ctx context.Context, md pdata.Metrics) error {
	p.logger.Debug("ConsumeMetrics")
	rm := md.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		ilm := rm.At(i).InstrumentationLibraryMetrics()
		for x := 0; x < ilm.Len(); x++ {
			metrics := ilm.At(x).Metrics()
			for y := 0; y < metrics.Len(); y++ {
				m := metrics.At(y)
				p.logger.Debug("metric", zap.String("name", m.Name()), zap.String("desc", m.Description()))
			}
		}
	}
	p.processedCh <- md.MetricCount()
	return nil
}

func (p *metricFlowProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *metricFlowProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Debug("process.Start")
	return nil
}

func (p *metricFlowProcessor) Shutdown(ctx context.Context) error {
	p.logger.Debug("process.Shutdown")
	return nil
}

func CreateMetricsProcessor(jchfCh chan []*kt.JCHF, processedCh chan int) func(_ context.Context, settings component.ProcessorCreateSettings, cfg config.Processor, nextConsumer consumer.Metrics) (component.MetricsProcessor, error) {
	return func(_ context.Context, settings component.ProcessorCreateSettings, cfg config.Processor, nextConsumer consumer.Metrics) (component.MetricsProcessor, error) {
		return NewMetricFlowProcessor(jchfCh, processedCh, settings.Logger, cfg, nextConsumer)
	}
}
