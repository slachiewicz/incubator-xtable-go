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

package hudi

import (
	"fmt"
	"path"
	"strings"
	"time"
)

// HudiInstantFormat is the Go layout for Hudi timeline instants (YYYYMMDDHHMMSSmmm).
const (
	HudiInstantFormat = "20060102150405000"
)

// InstantFromTime formats a time.Time into Hudi's timestamp string format (YYYYMMDDHHMMSSmmm).
func InstantFromTime(t time.Time) string {
	return t.UTC().Format(HudiInstantFormat)
}

// TimeFromInstant parses a Hudi instant string into time.Time.
func TimeFromInstant(instantStr string) (time.Time, error) {
	if len(instantStr) == 14 {
		// YYYYMMDDHHMMSS without millis
		return time.Parse("20060102150405", instantStr)
	}
	if len(instantStr) >= 17 {
		return time.Parse(HudiInstantFormat, instantStr[:17])
	}
	return time.Time{}, fmt.Errorf("invalid hudi instant format: %s", instantStr)
}

// HoodieWriteStat holds metadata for a data file written during a Hudi commit.
type HoodieWriteStat struct {
	FileID          string           `json:"fileId"`
	Path            string           `json:"path"`
	PrevCommit      string           `json:"prevCommit,omitempty"`
	NumWrites       int64            `json:"numWrites"`
	TotalWriteBytes int64            `json:"totalWriteBytes"`
	FileSizeInBytes int64            `json:"fileSizeInBytes"`
	MinValues       map[string]any   `json:"minValues,omitempty"`
	MaxValues       map[string]any   `json:"maxValues,omitempty"`
	NullCount       map[string]int64 `json:"nullCount,omitempty"`
}

// HoodieCommitMetadata represents the JSON body of a .commit, .deltacommit or .replacecommit
// timeline file.
type HoodieCommitMetadata struct {
	PartitionToWriteStats map[string][]HoodieWriteStat `json:"partitionToWriteStats"`
	// PartitionToReplaceFileIds lists, per partition, the file groups this instant supersedes. Only
	// a .replacecommit (INSERT_OVERWRITE, DELETE_PARTITION, clustering) carries it.
	PartitionToReplaceFileIds map[string][]string `json:"partitionToReplaceFileIds,omitempty"`
	ExtraMetadata             map[string]string   `json:"extraMetadata,omitempty"`
	OperationType             string              `json:"operationType"`
}

// FileGroupID returns the Hudi file group a write stat belongs to, falling back to the file name's
// leading component when the stat omits fileId. Hudi base file names are
// <fileId>_<writeToken>_<instantTime>.parquet, so the prefix identifies the group.
func (ws HoodieWriteStat) FileGroupID() string {
	if ws.FileID != "" {
		return ws.FileID
	}
	base := path.Base(ws.Path)
	if idx := strings.Index(base, "_"); idx > 0 {
		return base[:idx]
	}
	return base
}

// InstantAction represents the lifecycle state of an action in the Hudi timeline.
type InstantAction struct {
	InstantTime string
	Action      string // commit, deltacommit, clean, replacecommit
	State       string // requested, inflight, completed
	FileName    string
}
