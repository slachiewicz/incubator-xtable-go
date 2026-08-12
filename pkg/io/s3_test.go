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

	"github.com/slachiewicz/xtable-go/pkg/io"
)

func TestParseS3URI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uri         string
		wantBucket  string
		wantKey     string
		expectError bool
	}{
		{
			name:        "standard s3 uri with key",
			uri:         "s3://my-lakehouse-bucket/tables/users/_delta_log/000.json",
			wantBucket:  "my-lakehouse-bucket",
			wantKey:     "tables/users/_delta_log/000.json",
			expectError: false,
		},
		{
			name:        "s3a uri with key",
			uri:         "s3a://data-bucket/warehouse/iceberg/metadata/v1.metadata.json",
			wantBucket:  "data-bucket",
			wantKey:     "warehouse/iceberg/metadata/v1.metadata.json",
			expectError: false,
		},
		{
			name:        "root bucket uri",
			uri:         "s3://my-lakehouse-bucket",
			wantBucket:  "my-lakehouse-bucket",
			wantKey:     "",
			expectError: false,
		},
		{
			name:        "invalid scheme",
			uri:         "hdfs://namenode:8020/data",
			expectError: true,
		},
		{
			name:        "empty bucket",
			uri:         "s3://",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := io.ParseS3URI(tt.uri)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBucket, bucket)
				assert.Equal(t, tt.wantKey, key)
			}
		})
	}
}
