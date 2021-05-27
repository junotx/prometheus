package agent

import (
	"context"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/storage/wal"
)

type Options struct {
	WalDir              string
	RemoteFlushDeadline model.Duration
	TenantHeader        string
	TenantId            string

	Gatherer   prometheus.Gatherer
	Registerer prometheus.Registerer
}

type Agent struct {
	logger log.Logger
	opts   *Options
	ctx    context.Context

	promCfg config.Config
	mtx     sync.RWMutex

	walStorage    *wal.Storage
	remoteStorage *remote.Storage
	fanoutStorage storage.Storage

	scrapeManager          *scrape.Manager
	discoveryManagerScrape *discovery.Manager
	discoveryScrapeCtx     context.Context
	discoveryScrapeCancel  context.CancelFunc
}

func (a *Agent) ApplyConfig(conf *config.Config) error {
	err := a.remoteStorage.ApplyConfig(conf)
	if err != nil {
		return err
	}
	err = a.scrapeManager.ApplyConfig(conf)
	if err != nil {
		return err
	}
	c := make(map[string]discovery.Configs)
	for _, v := range conf.ScrapeConfigs {
		c[v.JobName] = v.ServiceDiscoveryConfigs
	}
	err = a.discoveryManagerScrape.ApplyConfig(c)
	if err != nil {
		return err
	}
	a.mtx.Lock()
	defer a.mtx.Unlock()
	a.promCfg = *conf
	return nil
}

func (a *Agent) Run(ctx context.Context) error {
	var g run.Group
	g.Add(func() error {
		<-ctx.Done()
		return nil
	}, func(_ error) {
	})

	// truncate wal loop
	{
		ctx, cancelFunc := context.WithCancel(context.Background())
		g.Add(func() error {
			a.truncateLoop(ctx)
			return nil
		}, func(err error) {
			cancelFunc()
		})
	}

	// scrape discovery manager
	g.Add(
		func() error {
			err := a.discoveryManagerScrape.Run()
			level.Info(a.logger).Log("msg", "Scrape discovery manager stopped")
			return err
		},
		func(err error) {
			level.Info(a.logger).Log("msg", "Stopping scrape discovery manager...")
			a.discoveryScrapeCancel()
		},
	)
	// Scrape manager.
	g.Add(
		func() error {
			err := a.scrapeManager.Run(a.discoveryManagerScrape.SyncCh())
			level.Info(a.logger).Log("msg", "Scrape manager stopped")
			return err
		},
		func(err error) {
			level.Info(a.logger).Log("msg", "Stopping scrape manager...")
			a.scrapeManager.Stop()
			level.Info(a.logger).Log("msg", "Closing storage...")
			if err := a.fanoutStorage.Close(); err != nil {
				level.Error(a.logger).Log("msg", "error Closing storage", "err", err)
			}
		},
	)
	return g.Run()
}

func (a *Agent) truncateLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			a.walStorage.Truncate(timestamp.FromTime(time.Now()))

		}
	}
}

func (a *Agent) getRemoteWriteTimestamp() (int64, error) {
	var labelValues []string

	a.mtx.RLock()
	var cs = a.promCfg.RemoteWriteConfigs
	a.mtx.RUnlock()
	for _, c := range cs {
		labelValues = append(labelValues, c.Name)
	}

	vals, err := getMetricValues(a.opts.Gatherer, "queue_highest_sent_timestamp_seconds", "remote_name", labelValues...)
	if err != nil {
		return 0, err
	}

	ts := int64(math.MaxInt64)
	for _, val := range vals {
		ival := int64(val)
		if ival < ts {
			ts = ival
		}
	}

	return ts, nil
}

// writeClientFunc
func (a *Agent) writeClientFunc(name string, conf *remote.ClientConfig) (remote.WriteClient, error) {
	c, err := remote.NewWriteClient(name, conf)
	if err != nil {
		return nil, err
	}
	rc := c.(*remote.Client)
	rc.Client.Transport = instrumentRoundTripperTenant(a.opts.TenantHeader, a.opts.TenantId, rc.Client.Transport)
	return c, nil
}

// New creates a new Agent
func New(logger log.Logger, opts *Options) (*Agent, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var (
		a = &Agent{
			logger: logger,
			opts:   opts,
		}
		scraper = &readyScrapeManager{}
		err     error
	)

	a.walStorage, err = wal.NewStorage(log.With(a.logger, "component", "wal"), a.opts.Registerer, a.opts.WalDir)
	if err != nil {
		return nil, err
	}
	a.remoteStorage = remote.NewStorageWithWriteClientFunc(log.With(a.logger, "component", "remote"),
		a.opts.Registerer, a.walStorage.StartTime, a.opts.WalDir, time.Duration(a.opts.RemoteFlushDeadline), scraper, a.writeClientFunc)
	a.fanoutStorage = storage.NewFanout(a.logger, a.walStorage, a.remoteStorage)

	a.scrapeManager = scrape.NewManager(log.With(a.logger, "component", "scrape manager"), a.fanoutStorage)
	scraper.Set(a.scrapeManager)

	a.discoveryScrapeCtx, a.discoveryScrapeCancel = context.WithCancel(context.Background())
	a.discoveryManagerScrape = discovery.NewManager(a.discoveryScrapeCtx,
		log.With(a.logger, "component", "discovery manager scrape"), discovery.Name("scrape"))

	return a, nil
}

// ErrNotReady is returned if the underlying scrape manager is not ready yet.
var ErrNotReady = errors.New("Scrape manager not ready")

// ReadyScrapeManager allows a scrape manager to be retrieved. Even if it's set at a later point in time.
type readyScrapeManager struct {
	mtx sync.RWMutex
	m   *scrape.Manager
}

// Set the scrape manager.
func (rm *readyScrapeManager) Set(m *scrape.Manager) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.m = m
}

// Get the scrape manager. If is not ready, return an error.
func (rm *readyScrapeManager) Get() (*scrape.Manager, error) {
	rm.mtx.RLock()
	defer rm.mtx.RUnlock()

	if rm.m != nil {
		return rm.m, nil
	}

	return nil, ErrNotReady
}

// instrumentRoundTripperTenant is a middleware that wraps the provided http.RoundTripper
// to add the provided header to a request
func instrumentRoundTripperTenant(tenantHeader, tenantId string, next http.RoundTripper) promhttp.RoundTripperFunc {
	return func(req *http.Request) (*http.Response, error) {
		if len(req.Header.Get(tenantHeader)) == 0 {
			req.Header.Set(tenantHeader, tenantId)
		}
		return next.RoundTrip(req)
	}
}

// Validate Agent Options
func Validate(opt *Options) error {

	// validate the tenant header to avoid overwriting those headers in remote.Client
	occupiedHeaders := []string{
		"Content-Encoding",
		"Content-Type",
		"User-Agent",
		"X-Prometheus-Remote-Write-Version",
	}
	for _, h := range occupiedHeaders {
		if opt.TenantHeader == h {
			return errors.Errorf("invalid tenant.header: %s. tenant.header can not be one of %s",
				opt.TenantHeader, strings.Join(occupiedHeaders, ","))
		}
	}

	return nil
}

func getMetricValues(gatherer prometheus.Gatherer, metricName, label string, labelValues ...string) ([]float64, error) {
	labelValueSet := make(map[string]bool)
	for _, v := range labelValues {
		labelValueSet[v] = true
	}

	families, err := gatherer.Gather()
	if err != nil {
		return nil, err
	}

	var vals []float64
	for _, family := range families {
		if !strings.Contains(family.GetName(), metricName) {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() != label {
					continue
				}
				if _, ok := labelValueSet[l.GetValue()]; ok {
					var value float64
					if m.Gauge != nil {
						value = m.Gauge.GetValue()
					} else if m.Counter != nil {
						value = m.Counter.GetValue()
					} else if m.Untyped != nil {
						value = m.Untyped.GetValue()
					} else {
						return nil, errors.New("unexpected metric type")
					}

					vals = append(vals, value)
					break
				}
			}
		}
	}

	return vals, nil
}
