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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/incubator-xtable-go/pkg/daemon"
	"github.com/apache/incubator-xtable-go/pkg/formats/delta"
	localio "github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
)

func TestE2E_DaemonRESTService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := t.TempDir()
	tableDir := filepath.Join(tmpDir, "orders_table")
	storage := localio.NewLocalStorage()

	// 1. Create Delta seed table
	idField := &model.Field{Name: "order_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	totalField := &model.Field{Name: "total", Schema: model.NewPrimitiveSchema(model.TypeDouble, false)}
	schema := model.NewRecordSchema("orders", []*model.Field{idField, totalField}, false)

	table := &model.Table{
		Name:             "orders",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         tableDir,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  filepath.Join(tableDir, "part-0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 2048,
		RecordCount:   100,
		LastModified:  time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	deltaTarget := delta.NewTarget(storage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Start HTTP Test Server
	server := daemon.NewServer("0.1.0-INTEGRATION")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	client := httpServer.Client()

	// 3. Test GET /v1/health
	t.Run("HealthCheck", func(t *testing.T) {
		resp, err := client.Get(httpServer.URL + "/v1/health")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var health daemon.HealthStatus
		err = json.NewDecoder(resp.Body).Decode(&health)
		require.NoError(t, err)
		assert.Equal(t, "UP", health.Status)
		assert.Equal(t, "0.1.0-INTEGRATION", health.Version)
	})

	// 4. Test POST /v1/conversion/inspect
	t.Run("InspectTable", func(t *testing.T) {
		inspectReq := daemon.InspectTableRequest{
			Format:        model.TableFormatDelta,
			TableBasePath: tableDir,
		}
		bodyBytes, _ := json.Marshal(inspectReq)
		resp, err := client.Post(httpServer.URL+"/v1/conversion/inspect", "application/json", bytes.NewReader(bodyBytes))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var inspectResp daemon.InspectTableResponse
		err = json.NewDecoder(resp.Body).Decode(&inspectResp)
		require.NoError(t, err)
		assert.Equal(t, "orders", inspectResp.TableName)
		assert.Equal(t, 1, inspectResp.ActiveDataFilesCount)
	})

	// 5. Test Synchronous POST /v1/conversion/table
	t.Run("ConvertTableSync", func(t *testing.T) {
		convReq := daemon.ConvertTableRequest{
			SourceFormat:  model.TableFormatDelta,
			TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
			TableName:     "orders",
			TableBasePath: tableDir,
		}
		bodyBytes, _ := json.Marshal(convReq)
		resp, err := client.Post(httpServer.URL+"/v1/conversion/table", "application/json", bytes.NewReader(bodyBytes))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		var convResp daemon.ConvertTableResponse
		err = json.NewDecoder(resp.Body).Decode(&convResp)
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", convResp.Status)
		assert.Len(t, convResp.Results, 2)
	})

	// 6. Test Asynchronous POST /v1/conversion/table + Poll GET /v1/conversion/table/{id}
	t.Run("ConvertTableAsyncAndPoll", func(t *testing.T) {
		convReq := daemon.ConvertTableRequest{
			SourceFormat:  model.TableFormatDelta,
			TargetFormats: []model.TableFormat{model.TableFormatIceberg},
			TableName:     "orders",
			TableBasePath: tableDir,
		}
		bodyBytes, _ := json.Marshal(convReq)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, httpServer.URL+"/v1/conversion/table", bytes.NewReader(bodyBytes))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Prefer", "respond-async")

		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
		var asyncResp daemon.ConvertTableResponse
		err = json.NewDecoder(resp.Body).Decode(&asyncResp)
		require.NoError(t, err)
		require.NotEmpty(t, asyncResp.ConversionID)

		// Poll until COMPLETED
		pollURL := fmt.Sprintf("%s/v1/conversion/table/%s", httpServer.URL, asyncResp.ConversionID)
		var finalResp daemon.ConvertTableResponse
		require.Eventually(t, func() bool {
			pollReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, pollURL, nil)
			pResp, pErr := client.Do(pollReq)
			if pErr != nil {
				return false
			}
			defer func() { _ = pResp.Body.Close() }()
			body, _ := io.ReadAll(pResp.Body)
			_ = json.Unmarshal(body, &finalResp)
			return finalResp.Status == "COMPLETED"
		}, 3*time.Second, 50*time.Millisecond)

		assert.Equal(t, "COMPLETED", finalResp.Status)
	})
}
