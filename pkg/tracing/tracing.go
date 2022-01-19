package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}

	return hostname
}

func tracerProvider(endpoint, service, environment string) (*tracesdk.TracerProvider, error) {
	var exporter tracesdk.SpanExporter
	if endpoint != "" {
		exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
		if err != nil {
			return nil, err
		}
		exporter = exp
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
			attribute.String("environment", environment),
			attribute.String("host", getHostname()),
		)),
	)
	return tp, nil
}

// NewProvider configures and OpenTelemetry tracing provider
func NewProvider(endpoint, serviceName, environment string) (*tracesdk.TracerProvider, error) {
	tp, err := tracerProvider(endpoint, serviceName, environment)
	if err != nil {
		return nil, err
	}

	// register global provider
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp, nil
}

// GetTraceSpan returns a trace.Span with the service and name configured
func GetTraceSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tCtx, sp := otel.Tracer("ktranslate").Start(ctx, name)
	return tCtx, sp
}
