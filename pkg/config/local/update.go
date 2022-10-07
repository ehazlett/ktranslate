package local

import (
	"context"

	"github.com/kentik/ktranslate"
)

func (l *Local) UpdateConfig(ctx context.Context, cfg *ktranslate.Config) error {
	// NOOP
	l.reloadCh <- cfg
	return nil
}
