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

package daemon_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/daemon"
	"github.com/apache/incubator-xtable-go/pkg/formats/delta"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
)

func TestServer_Health(t *testing.T) {
	t.Parallel()

	server := daemon.NewServer("0.1.0-TEST")
	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var health daemon.HealthStatus
	err := json.Unmarshal(w.Body.Bytes(), &health)
	require.NoError(t, err)
	assert.Equal(t, "UP", health.Status)
	assert.Equal(t, "0.1.0-TEST", health.Version)
}

func TestServer_ConvertTableSyncAndInspect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/daemon_test_table"

	// 1. Create Delta sample table
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema("products", []*model.Field{idField}, false)
	table := &model.Table{
		Name:             "products",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  "mem://lake/daemon_test_table/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   10,
		LastModified:  time.Now().UnixMilli(),
	}
	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	deltaTarget := delta.NewTarget(memStorage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	server := daemon.NewServerWithStorage("0.1.0", memStorage)

	// 2. Test Inspect Table
	inspectReq := daemon.InspectTableRequest{
		Format:        model.TableFormatDelta,
		TableBasePath: basePath,
	}
	inspectBody, _ := json.Marshal(inspectReq)
	req := httptest.NewRequest(http.MethodPost, "/v1/conversion/inspect", bytes.NewReader(inspectBody))
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var inspectResp daemon.InspectTableResponse
	err = json.Unmarshal(w.Body.Bytes(), &inspectResp)
	require.NoError(t, err)
	assert.Equal(t, "products", inspectResp.TableName)
	assert.Equal(t, 1, inspectResp.ActiveDataFilesCount)

	// 3. Test Synchronous Convert Table
	convertReq := daemon.ConvertTableRequest{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
		TableName:     "products",
		TableBasePath: basePath,
	}
	convertBody, _ := json.Marshal(convertReq)
	req = httptest.NewRequest(http.MethodPost, "/v1/conversion/table", bytes.NewReader(convertBody))
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var convertResp daemon.ConvertTableResponse
	err = json.Unmarshal(w.Body.Bytes(), &convertResp)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", convertResp.Status)
	assert.Len(t, convertResp.Results, 2)
}
