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

// Snapshot captures the complete state of a table at a specific point in time or commit.
type Snapshot struct {
	// Table is the metadata descriptor of the table at this snapshot instant.
	Table *Table `json:"table"`
	// PartitionedDataFiles groups data files by partition path.
	PartitionedDataFiles []*PartitionFileGroup `json:"partitionedDataFiles,omitempty"`
	// DataFiles holds a flat list of all active data files in this snapshot.
	DataFiles []*DataFile `json:"dataFiles,omitempty"`
	// SourceIdentifier is the format-specific commit version or timestamp string.
	SourceIdentifier string `json:"sourceIdentifier"`
}

// AllDataFiles returns a consolidated slice of all active data files in this snapshot.
func (s *Snapshot) AllDataFiles() []*DataFile {
	if len(s.DataFiles) > 0 {
		return s.DataFiles
	}
	var files []*DataFile
	for _, pfg := range s.PartitionedDataFiles {
		files = append(files, pfg.Files...)
	}
	return files
}
