package aggregate

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/query"
)

type countValuesOperator struct {
	model.OperatorTelemetry

	pool  *model.VectorPool
	next  model.VectorOperator
	param string

	stepsBatch int
	curStep    int

	ts        []int64
	counts    []map[float64]int
	series    []labels.Labels
	seriesIds map[float64]int

	once sync.Once
}

func NewCountValues(pool *model.VectorPool, next model.VectorOperator, param string, opts *query.Options) model.VectorOperator {
	return &countValuesOperator{
		pool:       pool,
		next:       next,
		param:      param,
		stepsBatch: opts.StepsBatch,
	}
}

func (c *countValuesOperator) Explain() []model.VectorOperator {
	return []model.VectorOperator{c.next}
}

func (c *countValuesOperator) GetPool() *model.VectorPool {
	return c.pool
}

func (c *countValuesOperator) String() string {
	return fmt.Sprintf("[countValues] '%s'", c.param)
}

func (c *countValuesOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	//start := time.Now()
	//defer func() { c.AddExecutionTimeTaken(time.Since(start)) }()

	var err error
	c.once.Do(func() { err = c.initSeriesOnce(ctx) })
	return c.series, err
}

func (c *countValuesOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	//start := time.Now()
	//defer func() { c.AddExecutionTimeTaken(time.Since(start)) }()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var err error
	c.once.Do(func() { err = c.initSeriesOnce(ctx) })
	if err != nil {
		return nil, err
	}

	if c.curStep >= len(c.ts) {
		return nil, nil
	}

	batch := c.pool.GetVectorBatch()
	for i := 0; i < c.stepsBatch; i++ {
		if c.curStep >= len(c.ts) {
			break
		}
		sv := c.pool.GetStepVector(c.ts[c.curStep])
		for k, v := range c.counts[c.curStep] {
			sv.AppendSample(c.pool, uint64(c.seriesIds[k]), float64(v))
		}
		batch = append(batch, sv)
		c.curStep++
	}
	return batch, nil
}

func (c *countValuesOperator) initSeriesOnce(ctx context.Context) error {
	_, err := c.next.Series(ctx)
	if err != nil {
		return err
	}
	ts := make([]int64, 0)
	counts := make([]map[float64]int, 0)
	series := make([]labels.Labels, 0)
	seriesIds := make(map[float64]int, 0)

	seenSeries := make(map[string]struct{})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		in, err := c.next.Next(ctx)
		if err != nil {
			return err
		}
		if in == nil {
			break
		}
		for i := range in {
			ts = append(ts, in[i].T)
			count := make(map[float64]int)
			for j := range in[i].Samples {
				count[in[i].Samples[j]]++
			}
			for k := range count {
				fval := strconv.FormatFloat(k, 'f', -1, 64)
				if _, ok := seenSeries[fval]; !ok {
					seenSeries[fval] = struct{}{}
					series = append(series, labels.FromStrings(c.param, fval))
					seriesIds[k] = len(series) - 1
				}
			}
			counts = append(counts, count)
		}
		c.next.GetPool().PutVectors(in)
	}

	c.ts = ts
	c.counts = counts
	c.series = series
	c.seriesIds = seriesIds

	return nil
}
