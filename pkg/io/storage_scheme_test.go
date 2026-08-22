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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

func TestNewStorageForPath_SchemeRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    any
		wantErr string
	}{
		{name: "s3", path: "s3://bucket/table", want: &io.S3Storage{}},
		{name: "s3a", path: "s3a://bucket/table", want: &io.S3Storage{}},
		{name: "memory", path: "mem://table", want: &io.MemoryStorage{}},
		{name: "file scheme", path: "file:///tmp/table", want: &io.LocalStorage{}},
		{name: "plain absolute path", path: "/tmp/table", want: &io.LocalStorage{}},
		{name: "plain relative path", path: "data/table", want: &io.LocalStorage{}},
		{name: "gcs is not misrouted to local", path: "gs://bucket/table", wantErr: `"gs://"`},
		{name: "azure abfss", path: "abfss://container@account.dfs.core.windows.net/table", wantErr: `"abfss://"`},
		{name: "hdfs", path: "hdfs://namenode/table", wantErr: `"hdfs://"`},
		{name: "https", path: "https://example.com/table", wantErr: `"https://"`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			storage, err := io.NewStorageForPath(context.Background(), tt.path)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, io.ErrInvalidPath)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, storage)
				return
			}
			require.NoError(t, err)
			assert.IsType(t, tt.want, storage)
		})
	}
}
