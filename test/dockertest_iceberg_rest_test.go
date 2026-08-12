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
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/catalog"
	"github.com/slachiewicz/xtable-go/pkg/model"
)

func TestDockertest_IcebergRESTCatalogSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "failed to connect to Docker daemon")

	err = pool.Client.Ping()
	require.NoError(t, err, "failed to ping Docker daemon")

	// 1. Run Tabular Iceberg REST Catalog container with 120s auto-expiry
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "tabulario/iceberg-rest",
		Tag:        "latest",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8181/tcp": {{HostIP: "127.0.0.1", HostPort: ""}},
		},
	})
	require.NoError(t, err, "failed to start Iceberg REST container")
	_ = resource.Expire(120)
	defer func() {
		_ = pool.Purge(resource)
	}()

	restPort := resource.GetPort("8181/tcp")
	restURL := fmt.Sprintf("http://127.0.0.1:%s", restPort)

	// 2. Wait for Iceberg REST Catalog readiness
	err = pool.Retry(func() error {
		resp, err := http.Get(fmt.Sprintf("%s/v1/config", restURL))
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("iceberg rest catalog returned status %d", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err, "iceberg rest catalog failed to become ready in time")

	ctx := context.Background()

	// Create namespace 'analytics' in Iceberg REST Catalog
	nsBody := []byte(`{"namespace": ["analytics"]}`)
	nsReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/v1/namespaces", restURL), bytes.NewReader(nsBody))
	require.NoError(t, err)
	nsReq.Header.Set("Content-Type", "application/json")
	nsResp, err := http.DefaultClient.Do(nsReq)
	require.NoError(t, err)
	_ = nsResp.Body.Close()

	// 3. Initialize Iceberg REST Catalog client pointing to REST Catalog
	catalogConfig := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "analytics",
		URI:          restURL,
	}

	client, err := catalog.NewIcebergRESTCatalogClient(catalogConfig)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// 4. Create sample canonical Table descriptor
	idField := &model.Field{Name: "account_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	nameField := &model.Field{Name: "holder_name", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	balanceField := &model.Field{Name: "balance", Schema: model.NewDecimalSchema(15, 2, false)}
	schema := model.NewRecordSchema("accounts", []*model.Field{idField, nameField, balanceField}, false)

	table := &model.Table{
		Name:             "accounts",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         "/tmp/warehouse/accounts",
		LatestCommitTime: time.Now().UnixMilli(),
	}

	// 5. Register table to Nessie Iceberg REST Catalog
	err = client.CreateOrUpdateTable(ctx, table, nil)
	require.NoError(t, err, "failed to register Iceberg table in Nessie REST catalog")

	assert.Equal(t, catalog.CatalogTypeIcebergREST, client.CatalogType())
}
