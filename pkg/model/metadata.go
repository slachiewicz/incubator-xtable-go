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

// TableSyncMetadata captures synchronization state embedded inside target table metadata/properties.
type TableSyncMetadata struct {
	// LastInstantSynced is the timestamp (epoch millis) of the latest successfully synced commit.
	LastInstantSynced int64 `json:"lastInstantSynced"`
	// InstantsToConsiderForNextSync tracks any pending or intermediate commits required for incremental continuity.
	InstantsToConsiderForNextSync []int64 `json:"instantsToConsiderForNextSync,omitempty"`
	// SourceFormat is the source format that was translated into this table.
	SourceFormat TableFormat `json:"sourceFormat,omitempty"`
	// TargetFormat is the format of the table storing this sync metadata.
	TargetFormat TableFormat `json:"targetFormat,omitempty"`
	// CustomProperties stores additional provider-specific sync attributes.
	CustomProperties map[string]string `json:"customProperties,omitempty"`
}

const (
	// MetadataPropertyPrefix is the key prefix for sync metadata stored in table properties. The
	// "xtable_" spelling is deliberate: it is the on-disk contract with Java-XTable-synced tables
	// and renaming it would break round-tripping against upstream.
	MetadataPropertyPrefix = "xtable_"
	// KeyLastInstantSynced is the property key for the last synced instant.
	KeyLastInstantSynced = "xtable_last_instant_synced"
	// KeySourceFormat is the property key for the source table format.
	KeySourceFormat = "xtable_source_format"
)
