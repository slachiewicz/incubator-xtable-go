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

package paimon

import "encoding/json"

// Snapshot represents an Apache Paimon snapshot JSON file (snapshot/snapshot-N). The field names
// are org.apache.paimon.Snapshot's FIELD_* constants (paimon-bundle 1.3.1).
type Snapshot struct {
	Version              int               `json:"version"`
	ID                   int64             `json:"id"`
	SchemaID             int64             `json:"schemaId"`
	BaseManifestList     string            `json:"baseManifestList,omitempty"`
	DeltaManifestList    string            `json:"deltaManifestList,omitempty"`
	CommitUser           string            `json:"commitUser,omitempty"`
	CommitIdentifier     int64             `json:"commitIdentifier,omitempty"`
	CommitKind           string            `json:"commitKind,omitempty"`
	TimeMillis           int64             `json:"timeMillis"`
	TotalRecordCount     *int64            `json:"totalRecordCount,omitempty"`
	DeltaRecordCount     *int64            `json:"deltaRecordCount,omitempty"`
	ChangelogRecordCount *int64            `json:"changelogRecordCount,omitempty"`
	LogOffset            map[string]int64  `json:"logOffset,omitempty"`
	Properties           map[string]string `json:"properties,omitempty"`
}

// ParseSnapshotJSON parses Apache Paimon snapshot JSON content.
func ParseSnapshotJSON(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
