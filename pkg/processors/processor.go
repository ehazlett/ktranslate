package processors

import (
	"context"

	"github.com/kentik/ktranslate/pkg/kt"
)

type Processor interface {
	Process(context.Context, *kt.Output) (*kt.Output, error)
}
