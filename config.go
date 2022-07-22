package ktranslate

import "github.com/BurntSushi/toml"

type Config struct {
	ListenAddr          string
	MappingFile         string
	UDRSFile            string
	GeoFile             string
	ASNFile             string
	ApplicationFile     string
	APIDevicesFile      string
	SNMPFile            string
	DNS                 string
	ProcessingThreads   int
	InputThreads        int
	MaxThreads          int
	Format              string
	FormatRollup        string
	Compression         string
	Sinks               string
	MaxFlows            int
	RollupInterval      int
	RollupAndAlpha      bool
	SampleRate          int
	SampleMin           int
	EnableSNMPDiscovery bool
	KentikEmail         string
	KentikPlan          int
	APIBaseURL          string
	SSLCertFile         string
	SSLKeyFile          string
	TagMapType          string
	VPCSource           string
	FlowSource          string
	EnableTeeLogs       bool
	SyslogListenAddr    string
	EnableHTTPInput     bool
	EnricherURL         string
	LogLevel            string

	AWSLambda        bool
	TagMapConfigFile string
}

func DefaultConfig() *Config {
	return &Config{
		ListenAddr:          "127.0.0.1:8081",
		MappingFile:         "",
		UDRSFile:            "",
		GeoFile:             "",
		ASNFile:             "",
		ApplicationFile:     "",
		APIDevicesFile:      "",
		SNMPFile:            "",
		DNS:                 "",
		ProcessingThreads:   1,
		InputThreads:        1,
		MaxThreads:          1,
		Format:              "flat_json",
		FormatRollup:        "",
		Compression:         "none",
		Sinks:               "stdout",
		MaxFlows:            10000,
		RollupInterval:      0,
		RollupAndAlpha:      false,
		SampleRate:          1,
		SampleMin:           1,
		EnableSNMPDiscovery: false,
		KentikEmail:         "",
		KentikPlan:          0,
		APIBaseURL:          "https://api.kentik.com",
		SSLCertFile:         "",
		SSLKeyFile:          "",
		TagMapType:          "",
		VPCSource:           "",
		FlowSource:          "",
		EnableTeeLogs:       false,
		SyslogListenAddr:    "",
		EnableHTTPInput:     false,
		EnricherURL:         "",
		LogLevel:            "info",
	}
}

func LoadConfig(configPath string) (*Config, error) {
	var cfg *Config
	if _, err := toml.DecodeFile(configPath, &cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
