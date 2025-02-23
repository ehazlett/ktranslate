package formats

import (
	"fmt"

	"github.com/kentik/ktranslate/pkg/eggs/logger"

	"github.com/kentik/ktranslate/pkg/formats/avro"
	"github.com/kentik/ktranslate/pkg/formats/carbon"
	"github.com/kentik/ktranslate/pkg/formats/elasticsearch"
	"github.com/kentik/ktranslate/pkg/formats/influx"
	"github.com/kentik/ktranslate/pkg/formats/json"
	"github.com/kentik/ktranslate/pkg/formats/kflow"
	"github.com/kentik/ktranslate/pkg/formats/netflow"
	"github.com/kentik/ktranslate/pkg/formats/nrm"
	"github.com/kentik/ktranslate/pkg/formats/prom"
	"github.com/kentik/ktranslate/pkg/formats/splunk"
	"github.com/kentik/ktranslate/pkg/kt"
	"github.com/kentik/ktranslate/pkg/rollup"
)

type Formatter interface {
	To([]*kt.JCHF, []byte) (*kt.Output, error)
	From(*kt.Output) ([]map[string]interface{}, error)
	Rollup([]rollup.Rollup) (*kt.Output, error)
}

type Format string

const (
	FORMAT_AVRO          Format = "avro"
	FORMAT_ELASTICSEARCH        = "elasticsearch"
	FORMAT_JSON                 = "json"
	FORMAT_JSON_FLAT            = "flat_json"
	FORMAT_NETFLOW              = "netflow"
	FORMAT_INFLUX               = "influx"
	FORMAT_CARBON               = "carbon"
	FORMAT_PROM                 = "prometheus"
	FORMAT_NR                   = "new_relic"
	FORMAT_NRM                  = "new_relic_metric"
	FORMAT_SPLUNK               = "splunk"
	FORMAT_KFLOW                = "kflow"
)

func NewFormat(format Format, log logger.Underlying, compression kt.Compression) (Formatter, error) {
	switch format {
	case FORMAT_AVRO:
		return avro.NewFormat(log, compression)
	case FORMAT_ELASTICSEARCH:
		return elasticsearch.NewFormat(log, compression)
	case FORMAT_JSON:
		return json.NewFormat(log, compression, false)
	case FORMAT_NETFLOW:
		return netflow.NewFormat(log, compression)
	case FORMAT_INFLUX:
		return influx.NewFormat(log, compression)
	case FORMAT_CARBON:
		return carbon.NewFormat(log, compression)
	case FORMAT_PROM:
		return prom.NewFormat(log, compression)
	case FORMAT_NR, FORMAT_JSON_FLAT:
		return json.NewFormat(log, compression, true)
	case FORMAT_NRM:
		return nrm.NewFormat(log, compression)
	case FORMAT_SPLUNK:
		return splunk.NewFormat(log, compression)
	case FORMAT_KFLOW:
		return kflow.NewFormat(log, compression)
	default:
		return nil, fmt.Errorf("You used an unsupported format: %v.", format)
	}
}
