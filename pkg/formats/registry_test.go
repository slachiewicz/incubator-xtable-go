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

package formats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestNewSource(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		format    model.TableFormat
		wantError bool
		errorMsg  string
	}{
		{
			name:      "Delta format",
			format:    model.TableFormatDelta,
			wantError: false,
		},
		{
			name:      "Iceberg format",
			format:    model.TableFormatIceberg,
			wantError: false,
		},
		{
			name:      "Hudi format",
			format:    model.TableFormatHudi,
			wantError: false,
		},
		{
			name:      "Parquet format",
			format:    model.TableFormatParquet,
			wantError: false,
		},
		{
			name:      "Paimon format",
			format:    model.TableFormatPaimon,
			wantError: false,
		},
		{
			name:      "Invalid format",
			format:    "INVALID_FORMAT",
			wantError: true,
			errorMsg:  "unsupported source table format",
		},
	}

	storage := io.NewMemoryStorage()

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			source, err := formats.NewSource(tc.format, storage, "mem://test")

			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorMsg)
				require.Nil(t, source)
			} else {
				require.NoError(t, err)
				require.NotNil(t, source)
				assert.Equal(t, tc.format, source.Format())
			}
		})
	}
}

func TestNewTarget(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		format    model.TableFormat
		wantError bool
		errorMsg  string
	}{
		{
			name:      "Delta format",
			format:    model.TableFormatDelta,
			wantError: false,
		},
		{
			name:      "Iceberg format",
			format:    model.TableFormatIceberg,
			wantError: false,
		},
		{
			name:      "Hudi format",
			format:    model.TableFormatHudi,
			wantError: false,
		},
		{
			name:      "Parquet format",
			format:    model.TableFormatParquet,
			wantError: false,
		},
		{
			name:      "Paimon format",
			format:    model.TableFormatPaimon,
			wantError: false,
		},
		{
			name:      "Invalid format",
			format:    "INVALID_FORMAT",
			wantError: true,
			errorMsg:  "unsupported target table format",
		},
	}

	storage := io.NewMemoryStorage()
	ctx := context.Background()

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, err := formats.NewTarget(ctx, tc.format, storage, "mem://test", "test_table")

			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errorMsg)
				require.Nil(t, target)
			} else {
				require.NoError(t, err)
				require.NotNil(t, target)
				assert.Equal(t, tc.format, target.Format())
			}
		})
	}
}

func TestSupportedSources(t *testing.T) {
	t.Parallel()

	sources := formats.SupportedSources()

	require.Len(t, sources, 5, "should support exactly 5 source formats")

	require.Contains(t, sources, model.TableFormatDelta, "should support Delta as source")
	require.Contains(t, sources, model.TableFormatIceberg, "should support Iceberg as source")
	require.Contains(t, sources, model.TableFormatHudi, "should support Hudi as source")
	require.Contains(t, sources, model.TableFormatParquet, "should support Parquet as source")
	require.Contains(t, sources, model.TableFormatPaimon, "should support Paimon as source")
}

func TestSupportedTargets(t *testing.T) {
	t.Parallel()

	targets := formats.SupportedTargets()

	require.Len(t, targets, 5, "should support exactly 5 target formats")

	require.Contains(t, targets, model.TableFormatDelta, "should support Delta as target")
	require.Contains(t, targets, model.TableFormatIceberg, "should support Iceberg as target")
	require.Contains(t, targets, model.TableFormatHudi, "should support Hudi as target")
	require.Contains(t, targets, model.TableFormatParquet, "should support Parquet as target")
	require.Contains(t, targets, model.TableFormatPaimon, "should support Paimon as target")
}
