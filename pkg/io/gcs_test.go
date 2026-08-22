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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

func TestParseGCSURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		uri        string
		wantBucket string
		wantObject string
		wantErr    string
	}{
		{
			name:       "standard gs uri with object",
			uri:        "gs://my-lakehouse-bucket/tables/users/_delta_log/000.json",
			wantBucket: "my-lakehouse-bucket",
			wantObject: "tables/users/_delta_log/000.json",
		},
		{
			name:       "root bucket uri",
			uri:        "gs://my-lakehouse-bucket",
			wantBucket: "my-lakehouse-bucket",
			wantObject: "",
		},
		{
			name:    "invalid scheme",
			uri:     "hdfs://namenode:8020/data",
			wantErr: "must start with gs://",
		},
		{
			name:    "s3 scheme is not gs",
			uri:     "s3://bucket/key",
			wantErr: "must start with gs://",
		},
		{
			name:    "empty bucket",
			uri:     "gs://",
			wantErr: "missing bucket name",
		},
		{
			name:    "empty bucket with path",
			uri:     "gs:///tables/users",
			wantErr: "missing bucket name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bucket, object, err := io.ParseGCSURI(tt.uri)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, io.ErrInvalidPath)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantObject, object)
		})
	}
}
