package flow

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
)

type traceFlowProcessor struct {
	nextConsumer consumer.Traces
	logger       *zap.Logger
	processedCh  chan int
}

var (
	_ component.TracesProcessor = (*traceFlowProcessor)(nil)
)

func NewTraceFlowProcessor(processedCh chan int, l *zap.Logger, cfg config.Processor, nextConsumer consumer.Traces) (*traceFlowProcessor, error) {
	logger := l.With(zap.String("processor", "traceFlow"))
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	return &traceFlowProcessor{
		logger:       logger,
		nextConsumer: nextConsumer,
		processedCh:  processedCh,
	}, nil
}

func (p *traceFlowProcessor) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	p.logger.Debug("ConsumeTraces")
	go p.processTraces(ctx, td)
	return nil
}

func (p *traceFlowProcessor) processTraces(ctx context.Context, td pdata.Traces) {
	ts := time.Now().UnixNano()
	rs := td.ResourceSpans()
	p.logger.Debug("processing traces", zap.Int64("ts", ts), zap.Int("number", rs.Len()))
	rsSpans := rs.Len()
	for i := 0; i < rsSpans; i++ {
		p.logger.Debug("p.processSpans", zap.Int("spans", i))

		ils := rs.At(i).InstrumentationLibrarySpans()
		for x := 0; x < ils.Len(); x++ {
			for y := 0; y < ils.At(x).Spans().Len(); y++ {
				p.logger.Debug("span", zap.String("span", fmt.Sprintf("%+v", ils.At(x).Spans().At(y))))
			}
		}
	}

	p.logger.Debug("trace spans", zap.Int("spans", rs.At(0).InstrumentationLibrarySpans().At(0).Spans().Len()))

	if err := p.nextConsumer.ConsumeTraces(ctx, td); err != nil {
		p.logger.Error("error sending traces to next consumer")
	}

	p.logger.Debug("process traces complete", zap.Int64("ts", ts))
}

func (p *traceFlowProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *traceFlowProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Debug("process.Start")
	return nil
}

func (p *traceFlowProcessor) Shutdown(ctx context.Context) error {
	p.logger.Debug("process.Shutdown")
	return nil
}

func CreateTraceProcessor(processedCh chan int) func(_ context.Context, settings component.ProcessorCreateSettings, cfg config.Processor, nextConsumer consumer.Traces) (component.TracesProcessor, error) {
	return func(_ context.Context, settings component.ProcessorCreateSettings, cfg config.Processor, nextConsumer consumer.Traces) (component.TracesProcessor, error) {
		return NewTraceFlowProcessor(processedCh, settings.Logger, cfg, nextConsumer)
	}
}
