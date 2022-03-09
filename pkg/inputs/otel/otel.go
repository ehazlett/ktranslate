package otel

import (
	"context"
	"fmt"

	go_metrics "github.com/kentik/go-metrics"

	"github.com/kentik/ktranslate/pkg/api"
	"github.com/kentik/ktranslate/pkg/eggs/logger"
	"github.com/kentik/ktranslate/pkg/kt"
)

type KentikOtelListener struct {
	logger.ContextL
	metrics   *OtelListenerMetric
	collector *collector
}

type OtelListenerMetric struct {
	Messages go_metrics.Meter
	Errors   go_metrics.Meter
}

func NewOtelListener(ctx context.Context, listenAddr string, log logger.Underlying, registry go_metrics.Registry, jchfChan chan []*kt.JCHF, apic *api.KentikApi) (*KentikOtelListener, error) {
	metrics := &OtelListenerMetric{
		Messages: go_metrics.GetOrRegisterMeter(fmt.Sprintf("otel_messages^force=true"), registry),
		Errors:   go_metrics.GetOrRegisterMeter(fmt.Sprintf("otel_errors^force=true"), registry),
	}

	collector, err := newCollector(jchfChan, metrics, log)
	if err != nil {
		return nil, err
	}

	kl := KentikOtelListener{
		ContextL:  logger.NewContextLFromUnderlying(logger.SContext{S: "kentik-otel"}, log),
		collector: collector,
		metrics:   metrics,
	}

	go kl.run(ctx, listenAddr)
	return &kl, nil
}

type basic struct {
	Fields    map[string]float64 `json:"fields"`
	Name      string             `json:"name"`
	Tags      map[string]string  `json:"tags"`
	Timestamp int64              `json:"timestamp"`
}

type batch struct {
	Metrics []basic `json:"metrics"`
}

func (l *KentikOtelListener) Close() {}

func (l *KentikOtelListener) HttpInfo() map[string]float64 {
	msgs := map[string]float64{
		"messages": l.metrics.Messages.Rate1(),
		"errors":   l.metrics.Errors.Rate1(),
	}
	return msgs
}

func (l *KentikOtelListener) run(ctx context.Context, host string) {
	l.Infof("otel collector ready on %s", host)

	if err := l.collector.start(); err != nil {
		l.Errorf(err.Error())
	}
}
