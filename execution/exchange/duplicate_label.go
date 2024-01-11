// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"
)

type pair struct{ a, b int }

type duplicateLabelCheckOperator struct {
	model.OperatorTelemetry

	once sync.Once
	next model.VectorOperator

	p []pair
	c []uint64
}

func NewDuplicateLabelCheck(next model.VectorOperator, opts *query.Options) model.VectorOperator {
	return &duplicateLabelCheckOperator{
		OperatorTelemetry: model.NewTelemetry("[duplicateLabelCheck]", opts.EnableAnalysis),
		next:              next,
	}
}

func (d *duplicateLabelCheckOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	start := time.Now()
	defer func() { d.AddExecutionTimeTaken(time.Since(start)) }()

	if err := d.init(ctx); err != nil {
		return nil, err
	}

	in, err := d.next.Next(ctx)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}

	if len(d.p) > 0 {
		for _, sv := range in {
			for _, sid := range sv.SampleIDs {
				d.c[sid] = 1
			}
		}
		for i := range d.p {
			if d.c[d.p[i].a] > 0 && d.c[d.p[i].b] > 0 {
				return nil, extlabels.ErrDuplicateLabelSet
			}
		}
	}

	return in, nil
}

func (d *duplicateLabelCheckOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := d.init(ctx); err != nil {
		return nil, err
	}
	return d.next.Series(ctx)
}

func (d *duplicateLabelCheckOperator) GetPool() *model.VectorPool {
	return d.next.GetPool()
}

func (d *duplicateLabelCheckOperator) Explain() (me string, next []model.VectorOperator) {
	return d.next.Explain()
}

func (d *duplicateLabelCheckOperator) init(ctx context.Context) error {
	var err error
	d.once.Do(func() {
		series, seriesErr := d.next.Series(ctx)
		if seriesErr != nil {
			err = seriesErr
			return
		}
		m := make(map[uint64]int, len(series))
		p := make([]pair, 0)
		c := make([]uint64, len(series))
		for i := range series {
			h := series[i].Hash()
			if j, ok := m[h]; ok {
				p = append(p, pair{a: i, b: j})
			} else {
				m[h] = i
			}
		}
		d.p = p
		d.c = c
	})

	return err
}
