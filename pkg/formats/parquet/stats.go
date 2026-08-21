// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package parquet

import (
	"math"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/slachiewicz/polytable/pkg/model"
)

// columnAggregate accumulates one leaf column's statistics across the row groups of a file.
type columnAggregate struct {
	path       string
	columnType parquet.Type
	numValues  int64
	numNulls   int64
	minValue   parquet.Value
	maxValue   parquet.Value
	hasBounds  bool
}

// ColumnStatsFromFooter reads the row-group statistics of a Parquet footer and aggregates them
// into one model.ColumnStat per leaf column: value and null counts are summed, and the bounds are
// the minimum of the per-row-group minima and the maximum of their maxima.
//
// Columns whose path does not resolve against the schema are skipped rather than treated as an
// error, because the schema describes the whole dataset and one file need not match it. The
// converse holds too, and is what keeps a merged schema honest: a column this file does not carry
// has no chunk here, so it contributes no statistics at all rather than a zero-valued entry.
// NumNaNs stays zero: a Parquet footer does not record a NaN count.
func ColumnStatsFromFooter(file *parquet.File, schema *model.Schema) []*model.ColumnStat {
	if file == nil || schema == nil {
		return nil
	}
	return columnStatsForSchema(footerAggregates(file), schema)
}

// footerAggregates folds the row-group statistics of a footer into one aggregate per leaf column,
// in the file's own column order. It is kept apart from the schema resolution so that a caller
// crawling a directory can read each file once, then resolve every file's statistics against the
// schema merged from all of them.
func footerAggregates(file *parquet.File) []*columnAggregate {
	if file == nil {
		return nil
	}

	paths := file.Schema().Columns()
	aggregates := make([]*columnAggregate, len(paths))

	for _, rowGroup := range file.RowGroups() {
		for _, chunk := range rowGroup.ColumnChunks() {
			fileChunk, ok := chunk.(*parquet.FileColumnChunk)
			if !ok {
				continue
			}
			idx := chunk.Column()
			if idx < 0 || idx >= len(aggregates) {
				continue
			}
			agg := aggregates[idx]
			if agg == nil {
				agg = &columnAggregate{path: strings.Join(paths[idx], "."), columnType: chunk.Type()}
				aggregates[idx] = agg
			}
			agg.numValues += fileChunk.NumValues()
			agg.numNulls += fileChunk.NullCount()

			minValue, maxValue, ok := fileChunk.Bounds()
			if !ok {
				continue
			}
			agg.merge(minValue, maxValue)
		}
	}

	present := make([]*columnAggregate, 0, len(aggregates))
	for _, agg := range aggregates {
		if agg != nil {
			present = append(present, agg)
		}
	}
	return present
}

// columnStatsForSchema types each aggregate after the schema's field, dropping the columns the
// schema does not describe.
func columnStatsForSchema(aggregates []*columnAggregate, schema *model.Schema) []*model.ColumnStat {
	if schema == nil {
		return nil
	}

	var stats []*model.ColumnStat
	for _, agg := range aggregates {
		field := schema.FieldByPath(agg.path)
		if field == nil {
			continue
		}

		stat := &model.ColumnStat{
			Field:       field,
			TotalValues: agg.numValues,
			NumNulls:    agg.numNulls,
		}
		if agg.hasBounds {
			minValue, hasMin := valueToModel(agg.minValue, field.Schema)
			maxValue, hasMax := valueToModel(agg.maxValue, field.Schema)
			if hasMin || hasMax {
				stat.Range = model.NewRange(minValue, maxValue)
			}
		}
		stats = append(stats, stat)
	}
	return stats
}

// merge folds one row group's bounds into the aggregate. Comparison is delegated to the column's
// parquet.Type so that the logical ordering of the column, not the physical byte ordering, decides.
func (a *columnAggregate) merge(minValue, maxValue parquet.Value) {
	if !a.hasBounds {
		a.minValue, a.maxValue, a.hasBounds = minValue.Clone(), maxValue.Clone(), true
		return
	}
	if a.columnType != nil {
		if a.columnType.Compare(minValue, a.minValue) < 0 {
			a.minValue = minValue.Clone()
		}
		if a.columnType.Compare(maxValue, a.maxValue) > 0 {
			a.maxValue = maxValue.Clone()
		}
	}
}

// valueToModel converts a parquet bound onto the canonical model, typed after the field schema.
//
// It reports false for a null bound, for a NaN float — a NaN bound prunes nothing and cannot be
// JSON-encoded by the Delta target — and for decimal columns, whose footer bounds hold the
// unscaled backing value that this port has no schema-driven rescaling for yet.
func valueToModel(value parquet.Value, schema *model.Schema) (any, bool) {
	if value.IsNull() || schema == nil {
		return nil, false
	}
	if schema.DataType == model.TypeDecimal {
		return nil, false
	}

	switch value.Kind() {
	case parquet.Boolean:
		return value.Boolean(), true
	case parquet.Int32:
		return value.Int32(), true
	case parquet.Int64:
		return value.Int64(), true
	case parquet.Float:
		f := value.Float()
		if math.IsNaN(float64(f)) {
			return nil, false
		}
		return f, true
	case parquet.Double:
		f := value.Double()
		if math.IsNaN(f) {
			return nil, false
		}
		return f, true
	case parquet.ByteArray, parquet.FixedLenByteArray:
		raw := value.ByteArray()
		if schema.DataType == model.TypeString || schema.DataType == model.TypeEnum {
			return string(raw), true
		}
		return append([]byte(nil), raw...), true
	default:
		// INT96 is the deprecated timestamp encoding; no canonical bound for it.
		return nil, false
	}
}
