package config

import (
	"flag"
)

type Config struct {
	HostsFile    string
	InitialDelay int
	CrashDelay   int
	Crash        bool
}

func ParseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.HostsFile, "h", "", "Path to the hosts file")
	flag.IntVar(&cfg.InitialDelay, "d", 0, "Initial delay in seconds (default: 0)")
	flag.IntVar(&cfg.CrashDelay, "c", 0, "Crash delay in seconds")
	flag.BoolVar(&cfg.Crash, "t", false, "Crash")

	flag.Parse()

	if cfg.HostsFile == "" || cfg.InitialDelay < 0 {
		flag.Usage()
		return nil
	}

	return cfg
}
