package dynatrace

import (
	"fmt"
	"strings"

	"github.com/kentik/ktranslate/pkg/formats/util"
	"github.com/kentik/ktranslate/pkg/kt"
	"github.com/kentik/ktranslate/pkg/rollup"

	"github.com/kentik/ktranslate/pkg/eggs/logger"
)

const (
	InstNameNetflowEvent = "netflow-events"
	InstNameVPCEvent     = "vpc-flow-events"
	InstNameAWSVPCEvent  = "aws-vpc-flow-events"
)

type dynatraceMetric struct {
	metricName  string
	metricValue interface{}
	metricType  string
	metricUnit  string
	dimensions  []string
}

type DynatraceFormat struct {
	logger.ContextL
}

func NewFormat(log logger.Underlying) (*DynatraceFormat, error) {
	ef := &DynatraceFormat{
		ContextL: logger.NewContextLFromUnderlying(logger.SContext{S: "dynatraceFormat"}, log),
	}

	return ef, nil
}

func (f *DynatraceFormat) To(msgs []*kt.JCHF, serBuf []byte) (*kt.Output, error) {
	sendMsgs := make([]dynatraceMetric, 0, len(msgs))
	for _, msg := range msgs {
		if msg.EventType == kt.KENTIK_EVENT_SNMP {
			continue
		}
		mm := msg.Flatten()
		strip(mm)

		for _, m := range []string{"in_bytes", "in_pkts", "out_bytes", "out_pkts"} {
			// lookup metric value if exists
			v, ok := mm[m]
			if !ok {
				continue
			}

			metricType := ""
			metricUnit := ""
			if strings.Contains(m, "pkts") {
				metricType = "count"
			}
			if strings.Contains(m, "bytes") {
				metricType = "gauge"
				metricUnit = "BytePerSecond"
			}

			dimensions := []string{}

			for _, d := range []string{
				"src_addr",
				"dst_addr",
				"dst_as_name",
				"src_as_name",
				"l4_src_port",
				"l4_dst_port",
				"src_endpoint",
				"dst_endpoint",
				"device_id",
			} {
				if v, ok := mm[d]; ok {
					dimensions = append(dimensions, fmt.Sprintf("%s=\"%v\"", d, v))
				}
			}

			d := dynatraceMetric{
				metricName:  m,
				metricValue: v,
				metricType:  metricType,
				metricUnit:  metricUnit,
				dimensions:  dimensions,
			}
			sendMsgs = append(sendMsgs, d)
		}
	}

	data, err := serialize(sendMsgs)
	if err != nil {
		return nil, err
	}

	return kt.NewOutputWithProvider(data, msgs[0].Provider, kt.EventOutput), nil
}

// Not supported
func (f *DynatraceFormat) From(raw *kt.Output) ([]map[string]interface{}, error) {
	values := make([]map[string]interface{}, 0)
	return values, nil
}

// Not supported
func (f *DynatraceFormat) Rollup(rolls []rollup.Rollup) (*kt.Output, error) {
	return nil, nil
}

func serialize(metrics []dynatraceMetric) ([]byte, error) {
	d := []string{}
	for _, m := range metrics {
		// metadata
		if m.metricType != "" && m.metricUnit != "" {
			d = append(d, fmt.Sprintf("#%s %s dt.meta.unit=\"%s\"", m.metricName, m.metricType, m.metricUnit))
		}
		dim := ""
		if len(m.dimensions) > 0 {
			dim = fmt.Sprintf(",%s", strings.Join(m.dimensions, ","))
		}
		d = append(d, fmt.Sprintf("%s%s %v", m.metricName, dim, m.metricValue))
	}

	return []byte(strings.Join(d, "\n")), nil
}

func strip(in map[string]interface{}) {
	for k, v := range in {
		switch tv := v.(type) {
		case string:
			if tv == "" || tv == "-" || tv == "--" {
				delete(in, k)
			}
		case int32:
			if tv == 0 {
				delete(in, k)
			}
		case int64:
			if tv == 0 {
				delete(in, k)
			}
		}
		if _, ok := util.DroppedAttrs[k]; ok {
			delete(in, k)
		}
	}
	in["instrumentation.provider"] = kt.InstProvider // Let them know who sent this.
	switch in["provider"] {
	case kt.ProviderVPC:
		switch in["kt.from"] {
		case kt.FromLambda:
			in["instrumentation.name"] = InstNameAWSVPCEvent
		default:
			in["instrumentation.name"] = InstNameVPCEvent
		}
	default:
		in["instrumentation.name"] = InstNameNetflowEvent
	}
	in["collector.name"] = kt.CollectorName
}
