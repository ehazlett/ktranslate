package local

import (
	"context"

	"github.com/kentik/ktranslate"
)

func (l *Local) GetConfig(ctx context.Context) (*ktranslate.Config, error) {
	return ktranslate.LoadConfig(l.configPath)
}
