package local

import (
	"fmt"
	"net/url"
	"os"

	"github.com/kentik/ktranslate"
)

type Local struct {
	configPath string
	reloadCh   chan *ktranslate.Config
}

func NewLocal(uri string, reloadCh chan *ktranslate.Config) (*Local, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "file" {
		return nil, fmt.Errorf("expected file:// uri")
	}

	configPath := u.Path

	if _, err := os.Stat(configPath); err != nil {
		return nil, fmt.Errorf("error getting config %s: %w", configPath)
	}

	return &Local{
		configPath: configPath,
		reloadCh:   reloadCh,
	}, nil
}
