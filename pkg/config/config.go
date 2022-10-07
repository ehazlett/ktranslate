package config

import (
	"context"
	"fmt"
	"strings"

	"github.com/kentik/ktranslate"
	"github.com/kentik/ktranslate/pkg/config/local"
)

type ConfigProvider interface {
	// GetConfig gets the configuration from the provider
	GetConfig(ctx context.Context) (*ktranslate.Config, error)
	// UpdateConfig updates the ktranslate configuration in the provider
	UpdateConfig(ctx context.Context, cfg *ktranslate.Config) error
}

func NewProvider(addr string, reloadConfigCh chan *ktranslate.Config) (ConfigProvider, error) {
	parts := strings.SplitN(addr, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid config provider format: expected <type>:<uri>")
	}
	name := parts[0]
	uri := parts[1]
	switch strings.ToLower(name) {
	case "local":
		return local.NewLocal(uri, reloadConfigCh)
	}

	return nil, fmt.Errorf("unsupported config provider specified: %s", name)
}
