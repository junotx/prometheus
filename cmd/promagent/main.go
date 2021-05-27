package main

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/agent"
	"gopkg.in/alecthomas/kingpin.v2"
)

type flagConfig struct {
	configFile string

	agent agent.Options

	webListenAddress string

	promlogConfig promlog.Config
}

func main() {
	if os.Getenv("DEBUG") != "" {
		runtime.SetBlockProfileRate(20)
		runtime.SetMutexProfileFraction(20)
	}

	cfg := flagConfig{
		agent: agent.Options{
			Registerer: prometheus.DefaultRegisterer,
			Gatherer:   prometheus.DefaultGatherer,
		},
		promlogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "The Prometheus monitoring agent")

	a.Version(version.Print("promagent"))

	a.HelpFlag.Short('h')

	a.Flag("cfg.file", "Prometheus configuration file path.").
		Default("prometheus.yml").StringVar(&cfg.configFile)

	a.Flag("web.listen-address", "Address to listen on for API and telemetry.").
		Default("0.0.0.0:9090").StringVar(&cfg.webListenAddress)

	a.Flag("wal.dir", "Base path for wal log.").
		Default("data/").StringVar(&cfg.agent.WalDir)

	a.Flag("tenant.header", "").
		Default("THANOS-TENANT").StringVar(&cfg.agent.TenantHeader)

	a.Flag("tenant.id", "").
		Default("default-tenant").StringVar(&cfg.agent.TenantId)

	promlogflag.AddFlags(a, &cfg.promlogConfig)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}
	err = agent.Validate(&cfg.agent)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, ""))
	}

	logger := promlog.New(&cfg.promlogConfig)

	ctx := context.Background()

	ag, err := agent.New(logger, &cfg.agent)
	if err != nil {

	}
	err = ag.ApplyConfig(nil)
	err = ag.Run(ctx)

	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	router := route.New()
	router.Post("/-/reload", func(resp http.ResponseWriter, req *http.Request) {
		err = reloadConfig(cfg.configFile, logger, ag.ApplyConfig)
		if err != nil {
			http.Error(resp, fmt.Sprintf("failed to reload config: %s", err), http.StatusInternalServerError)
		}
	})
}

func reloadConfig(filename string, logger log.Logger, apply func(*config.Config) error) error {
	conf, err := config.LoadFile(filename)
	if err != nil {
		return err
	}
	return apply(conf)
}
