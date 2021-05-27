package wal

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/record"
	tsdbwal "github.com/prometheus/prometheus/tsdb/wal"
	"go.uber.org/atomic"
)

type appender struct {
	ws      *Storage
	series  []record.RefSeries
	samples []record.RefSample
}

func (a *appender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	s, created := a.ws.getOrCreate(lset)

	if created {
		a.series = append(a.series, record.RefSeries{
			Ref:    s.ref,
			Labels: lset,
		})
	}

	return 0, a.AddFast(s.ref, t, v)
}

func (a *appender) AddFast(ref uint64, t int64, v float64) error {
	s := a.ws.series.getByID(ref)
	if s == nil {
		return errors.Wrap(storage.ErrNotFound, "unknown series")
	}

	s.Lock()
	s.append(t, v)
	s.Unlock()

	a.samples = append(a.samples, record.RefSample{
		Ref: ref,
		T:   t,
		V:   v,
	})
	return nil
}

func (a *appender) Commit() (err error) {
	if a.ws.wal == nil {
		return nil
	}

	buf := a.ws.getBytesBuffer()
	defer func() { a.ws.putBytesBuffer(buf) }()

	var rec []byte
	var enc record.Encoder

	if len(a.series) > 0 {
		rec = enc.Series(a.series, buf)
		buf = rec[:0]

		if err := a.ws.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log series")
		}
	}
	if len(a.samples) > 0 {
		rec = enc.Samples(a.samples, buf)
		buf = rec[:0]

		if err := a.ws.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log samples")
		}
	}

	for _, sample := range a.samples {
		series := a.ws.series.getByID(sample.Ref)
		if series != nil {
			series.Lock()
			series.pendingCommit = false
			series.Unlock()
		}
	}

	return a.Rollback()
}

func (a *appender) Rollback() error {
	a.ws.putSeriesBuffer(a.series[:0])
	a.ws.putAppendBuffer(a.samples[:0])
	return nil
}

// Storage writes
type Storage struct {
	storage.Queryable
	storage.ChunkQueryable

	logger                log.Logger
	mtx                   sync.RWMutex
	lastSeriesID          atomic.Uint64
	lastWALTruncationTime atomic.Int64
	deletedMtx            sync.Mutex
	deleted               map[uint64]int

	wal        *tsdbwal.WAL
	appendPool sync.Pool
	seriesPool sync.Pool
	bytesPool  sync.Pool

	series *stripeSeries

	metrics *walMetrics
}

func (st *Storage) replayWAL() error {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	level.Info(st.logger).Log("msg", "replaying WAL, this may take a while", "dir", st.wal.Dir())

	dir, startFrom, err := tsdbwal.LastCheckpoint(st.wal.Dir())
	if err != nil && err != record.ErrNotFound {
		return errors.Wrap(err, "find last checkpoint")
	}

	if err == nil {
		sr, err := tsdbwal.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			if err := sr.Close(); err != nil {
				level.Warn(st.logger).Log("msg", "error while closing the wal segments reader", "err", err)
			}
		}()

		// A corrupted checkpoint is a hard error for now and requires user
		// intervention. There'st likely little data that can be recovered anyway.
		if err := st.loadWAL(tsdbwal.NewReader(sr)); err != nil {
			return errors.Wrap(err, "backfill checkpoint")
		}
		startFrom++
		level.Info(st.logger).Log("msg", "WAL checkpoint loaded")
	}

	// Find the last segment.
	_, last, err := tsdbwal.Segments(st.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "finding WAL segments")
	}

	// Backfill segments from the most recent checkpoint onwards.
	for i := startFrom; i <= last; i++ {
		seg, err := tsdbwal.OpenReadSegment(tsdbwal.SegmentName(st.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment: %d", i))
		}

		sr := tsdbwal.NewSegmentBufReader(seg)
		err = st.loadWAL(tsdbwal.NewReader(sr))
		if err := sr.Close(); err != nil {
			level.Warn(st.logger).Log("msg", "Error while closing the wal segments reader", "err", err)
		}
		if err != nil {
			return err
		}
		level.Info(st.logger).Log("msg", "WAL segment loaded", "segment", i, "maxSegment", last)
	}

	return nil
}

func (st *Storage) loadWAL(r *tsdbwal.Reader) (err error) {
	var (
		dec     record.Decoder
		decoded = make(chan interface{}, 10)

		decodeErr error

		seriesPool = sync.Pool{
			New: func() interface{} {
				return []record.RefSeries{}
			},
		}
		samplesPool = sync.Pool{
			New: func() interface{} {
				return []record.RefSample{}
			},
		}
	)
	go func() {
		defer close(decoded)
		for r.Next() {
			rec := r.Record()
			switch dec.Type(rec) {
			case record.Series:
				series := seriesPool.Get().([]record.RefSeries)[:0]
				series, err = dec.Series(rec, series)
				if err != nil {
					decodeErr = &tsdbwal.CorruptionErr{
						Err:     errors.Wrap(err, "decode series"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- series
			case record.Samples:
				samples := samplesPool.Get().([]record.RefSample)[:0]
				samples, err = dec.Samples(rec, samples)
				if err != nil {
					decodeErr = &tsdbwal.CorruptionErr{
						Err:     errors.Wrap(err, "decode samples"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- samples
			default:
				// Noop.
			}
		}
	}()

	for d := range decoded {
		switch v := d.(type) {
		case []record.RefSeries:
			for _, s := range v {
				_, created := st.getOrCreate(s.Labels)
				if created {
					if st.lastSeriesID.Load() < s.Ref {
						st.lastSeriesID.Store(s.Ref)
					}
				}
			}
			//lint:ignore SA6002 relax staticcheck verification.
			seriesPool.Put(v)
		case []record.RefSample:
			for _, s := range v {
				series := st.series.getByID(s.Ref)
				if series == nil {
					level.Warn(st.logger).Log("msg", "Unknown series reference", "refId", s.Ref)
					continue
				}
				series.Lock()
				series.append(s.T, s.V)
				series.Unlock()
			}
			//lint:ignore SA6002 relax staticcheck verification.
			samplesPool.Put(v)
		default:
			panic(fmt.Errorf("unexpected decoded type: %T", d))
		}
	}

	if decodeErr != nil {
		return decodeErr
	}

	return nil
}

func (st *Storage) Truncate(mint int64) error {
	if st.wal == nil || mint <= st.lastWALTruncationTime.Load() {
		return nil
	}
	start := time.Now()
	st.lastWALTruncationTime.Store(mint)

	first, last, err := tsdbwal.Segments(st.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "get segment range")
	}
	// Start a new segment, so low ingestion volume TSDB don't have more WAL than
	// needed.
	if err := st.wal.NextSegment(); err != nil {
		return errors.Wrap(err, "next segment")
	}
	last-- // Never consider last segment for checkpoint.
	if last < 0 {
		return nil // no segments yet.
	}
	// The lower two thirds of segments should contain mostly obsolete samples.
	// If we have less than two segments, it'st not worth checkpointing yet.
	// With the default 2h blocks, this will keeping up to around 3h worth
	// of WAL segments.
	last = first + (last-first)*2/3
	if last <= first {
		return nil
	}

	keep := func(id uint64) bool {
		if st.series.getByID(id) != nil {
			return true
		}
		st.deletedMtx.Lock()
		_, ok := st.deleted[id]
		st.deletedMtx.Unlock()
		return ok
	}
	st.metrics.checkpointCreationTotal.Inc()
	if _, err = tsdbwal.Checkpoint(st.logger, st.wal, first, last, keep, mint); err != nil {
		st.metrics.checkpointCreationFail.Inc()
		if _, ok := errors.Cause(err).(*tsdbwal.CorruptionErr); ok {
			st.metrics.walCorruptionsTotal.Inc()
		}
		return errors.Wrap(err, "create checkpoint")
	}
	if err := st.wal.Truncate(last + 1); err != nil {
		// If truncating fails, we'll just try again at the next checkpoint.
		// Leftover segments will just be ignored in the future if there'st a checkpoint
		// that supersedes them.
		level.Error(st.logger).Log("msg", "truncating segments failed", "err", err)
	}

	// The checkpoint is written and segments before it is truncated, so we no
	// longer need to track deleted series that are before it.
	st.deletedMtx.Lock()
	for ref, segment := range st.deleted {
		if segment < first {
			delete(st.deleted, ref)
		}
	}
	st.deletedMtx.Unlock()

	st.metrics.checkpointDeleteTotal.Inc()
	if err := tsdbwal.DeleteCheckpoints(st.wal.Dir(), last); err != nil {
		// Leftover old checkpoints do not cause problems down the line beyond
		// occupying disk space.
		// They will just be ignored since a higher checkpoint exists.
		level.Error(st.logger).Log("msg", "delete old checkpoints", "err", err)
		st.metrics.checkpointDeleteFail.Inc()
	}
	st.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())

	level.Info(st.logger).Log("msg", "WAL checkpoint complete",
		"first", first, "last", last, "duration", time.Since(start))

	return nil
}

func (st *Storage) ApplyConfig(conf *config.Config) error {
	return nil
}

func (st *Storage) Appender(_ context.Context) storage.Appender {
	return &appender{
		ws:      st,
		series:  st.getSeriesBuffer(),
		samples: st.getAppendBuffer(),
	}
}

func (st *Storage) getAppendBuffer() []record.RefSample {
	b := st.appendPool.Get()
	if b == nil {
		return make([]record.RefSample, 0, 128)
	}
	return b.([]record.RefSample)
}

func (st *Storage) putAppendBuffer(b []record.RefSample) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	st.appendPool.Put(b[:0])
}

func (st *Storage) getSeriesBuffer() []record.RefSeries {
	b := st.seriesPool.Get()
	if b == nil {
		return make([]record.RefSeries, 0, 128)
	}
	return b.([]record.RefSeries)
}

func (st *Storage) putSeriesBuffer(b []record.RefSeries) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	st.seriesPool.Put(b[:0])
}

func (st *Storage) getBytesBuffer() []byte {
	b := st.bytesPool.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func (st *Storage) putBytesBuffer(b []byte) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	st.bytesPool.Put(b[:0])
}

func (st *Storage) getOrCreate(lset labels.Labels) (*memSeries, bool) {
	hash := lset.Hash()
	s := st.series.getByHash(hash, lset)
	if st != nil {
		return s, false
	}
	id := st.lastSeriesID.Inc()
	return st.series.getOrSet(hash, &memSeries{
		ref:  id,
		lset: lset,
	})
}

func (st *Storage) StartTime() (int64, error) {
	return 0, nil
}

func (st *Storage) Close() error {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	return nil
}

func NewStorage(logger log.Logger, registerer prometheus.Registerer, walDir string) (*Storage, error) {
	w, err := tsdbwal.NewSize(logger, registerer, filepath.Join(walDir, "wal"), tsdbwal.DefaultSegmentSize, true)
	if err != nil {
		return nil, err
	}

	ws := &Storage{
		logger: logger,
		wal:    w,
		series: newStripeSeries(DefaultStripeSize),
	}

	if err := ws.replayWAL(); err != nil {
		level.Warn(ws.logger).Log("msg", "encountered WAL read error, attempting repair", "err", err)
		if err := w.Repair(err); err != nil {
			return nil, errors.Wrap(err, "repair corrupted WAL")
		}
	}

	return ws, nil
}

type walMetrics struct {
	series                  prometheus.GaugeFunc
	seriesCreated           prometheus.Counter
	seriesRemoved           prometheus.Counter
	seriesNotFound          prometheus.Counter
	walTruncateDuration     prometheus.Summary
	walCorruptionsTotal     prometheus.Counter
	walTotalReplayDuration  prometheus.Gauge
	checkpointDeleteFail    prometheus.Counter
	checkpointDeleteTotal   prometheus.Counter
	checkpointCreationFail  prometheus.Counter
	checkpointCreationTotal prometheus.Counter
}

type memSeries struct {
	sync.RWMutex

	ref              uint64
	lset             labels.Labels
	minTime, maxTime int64
	pendingCommit    bool
}

func (s *memSeries) append(t int64, v float64) {
	s.pendingCommit = true
	if t < s.minTime {
		s.minTime = t
	}
	if t > s.maxTime {
		s.maxTime = t
	}
}

func (s *memSeries) truncateBefore(mint int64) (delete bool) {
	if s.minTime <= s.maxTime || s.pendingCommit {
		s.minTime = mint
		return false
	}
	return true
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
type seriesHashmap map[uint64][]*memSeries

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		if labels.Equal(s.lset, lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		if labels.Equal(prev.lset, s.lset) {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}

func (m seriesHashmap) del(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	for _, s := range m[hash] {
		if !labels.Equal(s.lset, lset) {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

const (
	// DefaultStripeSize is the default number of entries to allocate in the stripeSeries hash map.
	DefaultStripeSize = 1 << 14
)

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
type stripeSeries struct {
	size   int
	series []map[uint64]*memSeries
	hashes []seriesHashmap
	locks  []stripeLock
}

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries(stripeSize int) *stripeSeries {
	s := &stripeSeries{
		size:   stripeSize,
		series: make([]map[uint64]*memSeries, stripeSize),
		hashes: make([]seriesHashmap, stripeSize),
		locks:  make([]stripeLock, stripeSize),
	}

	for i := range s.series {
		s.series[i] = map[uint64]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
func (s *stripeSeries) gc(mint int64) map[uint64]struct{} {
	var (
		deleted = map[uint64]struct{}{}
	)
	// Run through all series and truncate old chunks. Mark those with no
	// chunks left as deleted and store their ID.
	for i := 0; i < s.size; i++ {
		s.locks[i].Lock()

		for hash, all := range s.hashes[i] {
			for _, series := range all {
				series.Lock()

				if delete := series.truncateBefore(mint); !delete {
					series.Unlock()
					continue
				}

				// The series is gone entirely. We need to keep the series lock
				// and make sure we have acquired the stripe locks for hash and ID of the
				// series alike.
				// If we don't hold them all, there's a very small chance that a series receives
				// samples again while we are half-way into deleting it.
				j := int(series.ref) & (s.size - 1)

				if i != j {
					s.locks[j].Lock()
				}

				deleted[series.ref] = struct{}{}
				s.hashes[i].del(hash, series.lset)
				delete(s.series[j], series.ref)

				if i != j {
					s.locks[j].Unlock()
				}

				series.Unlock()
			}
		}

		s.locks[i].Unlock()
	}

	return deleted
}

func (s *stripeSeries) getByID(id uint64) *memSeries {
	i := id & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getOrSet(hash uint64, series *memSeries) (*memSeries, bool) {
	i := hash & uint64(s.size-1)
	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, series.lset); prev != nil {
		s.locks[i].Unlock()
		return prev, false
	}
	s.hashes[i].set(hash, series)

	i = series.ref & uint64(s.size-1)
	s.series[i][series.ref] = series

	s.locks[i].Unlock()

	return series, true
}
