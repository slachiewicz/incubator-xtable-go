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

// DeletionVector represents row deletion metadata associated with a data file (e.g. Delta/Iceberg).
type DeletionVector struct {
	// StoragePath is the relative or absolute path to the deletion vector file if stored externally.
	StoragePath string `json:"storagePath,omitempty"`
	// Offset is the byte offset where the deletion vector bitmap starts.
	Offset int64 `json:"offset,omitempty"`
	// SizeInBytes is the byte length of the deletion vector.
	SizeInBytes int64 `json:"sizeInBytes"`
	// Cardinality is the number of deleted records marked by this vector.
	Cardinality int64 `json:"cardinality"`
	// InlineBytes contains raw bitmap bytes if stored inline within metadata.
	InlineBytes []byte `json:"inlineBytes,omitempty"`
}

// DataFile represents a physical data file in the table.
type DataFile struct {
	// PhysicalPath is the fully qualified URI or relative path to the physical data file.
	PhysicalPath string `json:"physicalPath"`
	// FileFormat is the physical storage format (e.g. APACHE_PARQUET, APACHE_ORC).
	FileFormat FileFormat `json:"fileFormat"`
	// FileSizeBytes is the size of the data file in bytes.
	FileSizeBytes int64 `json:"fileSizeBytes"`
	// RecordCount is the total number of records contained in this file.
	RecordCount int64 `json:"recordCount"`
	// PartitionValues specifies the partition values for this file.
	PartitionValues []*PartitionValue `json:"partitionValues,omitempty"`
	// ColumnStats holds column-level min/max, null count, and NaN statistics.
	ColumnStats []*ColumnStat `json:"columnStats,omitempty"`
	// LastModified is the file last modified timestamp in milliseconds since epoch.
	LastModified int64 `json:"lastModified"`
	// DeletionVector optionally points to row deletion vector metadata.
	DeletionVector *DeletionVector `json:"deletionVector,omitempty"`
}

// PartitionFileGroup groups data files by partition path.
type PartitionFileGroup struct {
	// PartitionPath is the directory partition path (e.g. "date=2026-08-12").
	PartitionPath string `json:"partitionPath"`
	// Files is the list of data files belonging to this partition.
	Files []*DataFile `json:"files"`
}
