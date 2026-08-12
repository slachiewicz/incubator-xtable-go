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

// TableChange captures the delta modifications applied in a single commit or instant on the source table.
type TableChange struct {
	// FilesDiff records the data files added and removed in this change.
	FilesDiff *FilesDiff `json:"filesDiff"`
	// TableAsOfChange is the table metadata (schema, partitioning) as of this commit.
	TableAsOfChange *Table `json:"tableAsOfChange"`
	// SourceIdentifier is the unique commit version/instant identifier (e.g. Delta version "5", Hudi timestamp).
	SourceIdentifier string `json:"sourceIdentifier"`
	// CommitTime is the commit timestamp in milliseconds since epoch.
	CommitTime int64 `json:"commitTime"`
}

// IncrementalTableChanges represents an ordered sequence of commits for incremental synchronization.
type IncrementalTableChanges struct {
	// TableChanges contains the sequential list of commits to be replayed on target formats.
	TableChanges []*TableChange `json:"tableChanges"`
	// CurrentTable is the latest metadata state of the source table.
	CurrentTable *Table `json:"currentTable"`
}
