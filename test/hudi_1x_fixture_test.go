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

package test_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// The hudi-1.x fixture was written by Apache Hudi 1.2.0 through Spark 3.5: hoodie.table.version=9,
// timeline under .hoodie/timeline/ with completion-time instant names. polytable reads the 0.14-era
// layout only, and before the version guard existed this table was silently read as an EMPTY table
// (zero files, empty schema, exit 0) — a sync would have "succeeded" with empty target metadata.
// This test pins the loud refusal instead. When T37 implements the 1.x timeline, this test flips
// red on purpose: replace it with real read assertions against the manifest.
func TestHudi1x_ReadIsRefusedLoudly(t *testing.T) {
	t.Parallel()
	tableDir, manifest := loadFixture(t, "hudi-1.x")
	require.Equal(t, "HUDI", manifest.Format)

	source, err := formats.NewSource(model.TableFormatHudi, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	defer func() { _ = source.Close() }()

	_, err = source.GetCurrentTable(context.Background())
	require.Error(t, err, "a Hudi 1.x table must be refused, not read as empty")
	assert.Contains(t, err.Error(), "Hudi 1.x")
}
