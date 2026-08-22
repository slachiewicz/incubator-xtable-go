//go:build !js

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

package io_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	storagev1 "google.golang.org/api/storage/v1"

	polytableio "github.com/slachiewicz/polytable/pkg/io"
)

// listObjectsResponse is a hand-written but field-faithful storage.objects.list JSON response
// (google.golang.org/api/storage/v1's Objects/Object shape), covering an ordinary data file and a
// trailing-slash placeholder object — the only signal GCS gives for a "directory", since it never
// materializes one as a real object the way ADLS Gen2 does.
const listObjectsResponse = `{
  "kind": "storage#objects",
  "items": [
    {
      "kind": "storage#object",
      "name": "tables/t1/region=EU/",
      "bucket": "lakehouse-e2e",
      "size": "0",
      "updated": "2024-01-01T00:00:00Z"
    },
    {
      "kind": "storage#object",
      "name": "tables/t1/region=EU/data-001.parquet",
      "bucket": "lakehouse-e2e",
      "size": "1024",
      "updated": "2024-01-02T00:00:00Z"
    }
  ]
}`

// TestGCSStorage_List exercises List against a hand-written storage.objects.list JSON response
// served over httptest, standing in for fake-gcs-server: the response shape (Objects/Object, per
// google.golang.org/api/storage/v1) is what the JSON transport parses regardless of which server
// produced it, so an httptest server pins the same contract without a Docker dependency.
func TestGCSStorage_List(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/o") {
			http.NotFound(w, r)
			return
		}
		assert.Equal(t, "tables/t1/", r.URL.Query().Get("prefix"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(listObjectsResponse))
	}))
	defer server.Close()

	svc, err := storagev1.NewService(context.Background(),
		option.WithEndpoint(server.URL+"/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)

	gcs := polytableio.NewGCSStorageWithClient(svc)
	defer func() { _ = gcs.Close() }()

	results, err := gcs.List(context.Background(), "gs://lakehouse-e2e/tables/t1/")
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, "gs://lakehouse-e2e/tables/t1/region=EU/", results[0].Path)
	assert.True(t, results[0].IsDir)
	assert.Equal(t, int64(0), results[0].Size)

	assert.Equal(t, "gs://lakehouse-e2e/tables/t1/region=EU/data-001.parquet", results[1].Path)
	assert.False(t, results[1].IsDir)
	assert.Equal(t, int64(1024), results[1].Size)
}
