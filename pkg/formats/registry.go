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
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package formats

import (
	"context"
	"fmt"

	"github.com/slachiewicz/xtable-go/pkg/formats/delta"
	"github.com/slachiewicz/xtable-go/pkg/formats/hudi"
	"github.com/slachiewicz/xtable-go/pkg/formats/iceberg"
	"github.com/slachiewicz/xtable-go/pkg/formats/paimon"
	"github.com/slachiewicz/xtable-go/pkg/formats/parquet"
	"github.com/slachiewicz/xtable-go/pkg/io"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// NewSource creates a ConversionSource for the given format.
func NewSource(format model.TableFormat, storage io.Storage, basePath string) (spi.ConversionSource, error) {
	switch format {
	case model.TableFormatDelta:
		return delta.NewSource(storage, basePath), nil
	case model.TableFormatIceberg:
		return iceberg.NewSource(storage, basePath), nil
	case model.TableFormatHudi:
		return hudi.NewSource(storage, basePath), nil
	case model.TableFormatParquet:
		return parquet.NewSource(storage, basePath), nil
	case model.TableFormatPaimon:
		return paimon.NewSource(storage, basePath), nil
	default:
		return nil, fmt.Errorf("unsupported source table format: %s", format)
	}
}

// NewTarget creates and initializes a ConversionTarget for the given format.
func NewTarget(ctx context.Context, format model.TableFormat, storage io.Storage, basePath, tableName string) (spi.ConversionTarget, error) {
	targetTable := &model.Table{
		Name:        tableName,
		TableFormat: format,
		BasePath:    basePath,
	}

	var target spi.ConversionTarget

	switch format {
	case model.TableFormatDelta:
		target = delta.NewTarget(storage)
	case model.TableFormatIceberg:
		target = iceberg.NewTarget(storage)
	case model.TableFormatHudi:
		target = hudi.NewTarget(storage)
	case model.TableFormatParquet:
		target = parquet.NewTarget(storage)
	case model.TableFormatPaimon:
		target = paimon.NewTarget(storage)
	default:
		return nil, fmt.Errorf("unsupported target table format: %s", format)
	}

	if err := target.Init(ctx, targetTable); err != nil {
		return nil, err
	}

	return target, nil
}

// SupportedSources returns all formats with source implementations.
func SupportedSources() []model.TableFormat {
	return []model.TableFormat{
		model.TableFormatDelta,
		model.TableFormatIceberg,
		model.TableFormatHudi,
		model.TableFormatParquet,
		model.TableFormatPaimon,
	}
}

// SupportedTargets returns all formats with target implementations.
func SupportedTargets() []model.TableFormat {
	return []model.TableFormat{
		model.TableFormatDelta,
		model.TableFormatIceberg,
		model.TableFormatHudi,
		model.TableFormatParquet,
		model.TableFormatPaimon,
	}
}
