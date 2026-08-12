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

package spi

import (
	"time"

	"github.com/slachiewicz/xtable-go/pkg/model"
)

// SyncMode represents whether a sync operation is a full snapshot rebuild or incremental delta replay.
type SyncMode string

// Supported sync modes.
const (
	SyncModeFull        SyncMode = "FULL"
	SyncModeIncremental SyncMode = "INCREMENTAL"
)

// SyncStatusCode represents the execution status code of a format sync.
type SyncStatusCode string

// Terminal status codes for a format sync.
const (
	SyncStatusSuccess SyncStatusCode = "SUCCESS"
	SyncStatusError   SyncStatusCode = "ERROR"
	SyncStatusSkipped SyncStatusCode = "SKIPPED"
)

// SyncResult captures the output and status of syncing to a target format.
type SyncResult struct {
	// TableFormat is the target format that was updated.
	TableFormat model.TableFormat `json:"tableFormat"`
	// StatusCode is SUCCESS, ERROR, or SKIPPED.
	StatusCode SyncStatusCode `json:"statusCode"`
	// Error contains the error if sync failed.
	Error string `json:"error,omitempty"`
	// LastInstantSynced is the timestamp of the latest synced commit.
	LastInstantSynced int64 `json:"lastInstantSynced"`
	// Duration is the total time spent executing the sync.
	Duration time.Duration `json:"duration"`
}

// NewSuccessSyncResult creates a success sync result.
func NewSuccessSyncResult(format model.TableFormat, instant int64, duration time.Duration) *SyncResult {
	return &SyncResult{
		TableFormat:       format,
		StatusCode:        SyncStatusSuccess,
		LastInstantSynced: instant,
		Duration:          duration,
	}
}

// NewErrorSyncResult creates an error sync result.
func NewErrorSyncResult(format model.TableFormat, err error, duration time.Duration) *SyncResult {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	return &SyncResult{
		TableFormat: format,
		StatusCode:  SyncStatusError,
		Error:       errMsg,
		Duration:    duration,
	}
}
