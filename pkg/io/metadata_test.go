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

	"github.com/slachiewicz/polytable/pkg/io"
)

func TestIsMetadataPathComponent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{"_delta_log", true},
		{"_polytable_metadata", true},
		{"_temporary", true},
		{"_SUCCESS", true},
		{".hoodie", true},
		{".hidden", true},
		{"metadata", true},
		{"schema", true},
		{"snapshot", true},
		{"manifest", true},
		// Real data: a Hive partition segment, a data file, a partition value that happens to
		// share a name with a metadata directory (the "=" is what makes it a partition, not the
		// bare word).
		{"region=east", false},
		{"country=US", false},
		{"part-00000.parquet", false},
		{"region=metadata", false},
		{"data", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, io.IsMetadataPathComponent(tt.name))
		})
	}
}

func TestIsMetadataPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{"top-level metadata dir", "_delta_log/00000000000000000000.json", true},
		{"checkpoint parquet under the log", "_delta_log/00000000000000000010.checkpoint.parquet", true},
		{"iceberg metadata", "metadata/v1.metadata.json", true},
		{"hudi timeline", ".hoodie/hoodie.properties", true},
		{"parquet target metadata", "_polytable_metadata/manifest.json", true},
		{"hadoop staging", "_temporary/0/task_0/part-00000.parquet", true},
		{"hadoop success marker", "_SUCCESS", true},
		{"paimon schema", "schema/schema-0", true},
		{"paimon snapshot", "snapshot/snapshot-1", true},
		{"paimon manifest", "manifest/manifest-list-abc-0", true},

		// The bug this exists to fix: a suffix-only check on the file's own base name would pass
		// this. Only checking every component in the path catches it.
		{"metadata dir nested under a real-looking data dir", "data/_delta_log/x.parquet", true},
		{"metadata dir several levels deep", "a/b/c/.hoodie/hoodie.properties", true},

		{"real data file at the root", "part-00000.parquet", false},
		{"real hive partition", "region=east/part-00000.snappy.parquet", false},
		{"nested real hive partition", "country=US/city=NYC/part-00000.parquet", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, io.IsMetadataPath(tt.path))
		})
	}
}
