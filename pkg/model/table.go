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

package model

// DataLayoutStrategy describes how data files are structured on physical storage.
type DataLayoutStrategy string

const (
	// DataLayoutDirHierarchical represents hierarchical slash-separated paths (e.g. /2026/08/12/).
	DataLayoutDirHierarchical DataLayoutStrategy = "DIR_HIERARCHICAL"
	// DataLayoutHiveStyle represents standard Hive key=value partition paths (e.g. /year=2026/month=08/).
	DataLayoutHiveStyle DataLayoutStrategy = "HIVE_STYLE"
	// DataLayoutFlat represents unpartitioned flat storage under table data path.
	DataLayoutFlat DataLayoutStrategy = "FLAT"
)

// Table represents the canonical reference and current metadata state of a lakehouse table.
type Table struct {
	// Name is the logical table name.
	Name string `json:"name"`
	// TableFormat indicates the underlying format (HUDI, ICEBERG, DELTA, PAIMON, PARQUET).
	TableFormat TableFormat `json:"tableFormat"`
	// ReadSchema is the current canonical schema for reading data from this table.
	ReadSchema *Schema `json:"readSchema"`
	// LayoutStrategy is the data directory layout strategy.
	LayoutStrategy DataLayoutStrategy `json:"layoutStrategy,omitempty"`
	// BasePath is the root directory path containing table metadata and data.
	BasePath string `json:"basePath"`
	// DataPath is the path containing data files (defaults to BasePath if empty).
	DataPath string `json:"dataPath,omitempty"`
	// PartitioningFields contains the ordered list of partition field definitions.
	PartitioningFields []*PartitionField `json:"partitioningFields,omitempty"`
	// LatestCommitTime is the timestamp of the latest write/commit in milliseconds since epoch.
	LatestCommitTime int64 `json:"latestCommitTime"`
	// LatestMetadataPath is the path to the latest metadata descriptor file.
	LatestMetadataPath string `json:"latestMetadataPath,omitempty"`
}

// GetDataPath returns the effective data directory path (falling back to BasePath).
func (t *Table) GetDataPath() string {
	if t.DataPath != "" {
		return t.DataPath
	}
	return t.BasePath
}

// IsPartitioned returns true if the table has one or more partitioning fields.
func (t *Table) IsPartitioned() bool {
	return len(t.PartitioningFields) > 0
}
