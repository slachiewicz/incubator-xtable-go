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

package spi_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

func TestNewSuccessSyncResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   model.TableFormat
		instant  int64
		duration time.Duration
	}{
		{name: "iceberg", format: model.TableFormatIceberg, instant: 1700000000000, duration: 250 * time.Millisecond},
		{name: "delta", format: model.TableFormatDelta, instant: 0, duration: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := spi.NewSuccessSyncResult(tt.format, tt.instant, tt.duration)

			assert.Equal(t, tt.format, got.TableFormat)
			assert.Equal(t, spi.SyncStatusSuccess, got.StatusCode)
			assert.Equal(t, tt.instant, got.LastInstantSynced)
			assert.Equal(t, tt.duration, got.Duration)
			assert.Empty(t, got.Error, "a success result must carry no error text")
		})
	}
}

func TestNewErrorSyncResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   model.TableFormat
		err      error
		duration time.Duration
		wantErr  string
	}{
		{
			name:     "wrapped error is flattened to its message",
			format:   model.TableFormatHudi,
			err:      errors.New("commit timeline unreadable"),
			duration: 10 * time.Millisecond,
			wantErr:  "commit timeline unreadable",
		},
		{
			// The nil branch exists so callers cannot panic building a failure result.
			name:     "nil error yields an empty message rather than panicking",
			format:   model.TableFormatPaimon,
			err:      nil,
			duration: time.Second,
			wantErr:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := spi.NewErrorSyncResult(tt.format, tt.err, tt.duration)

			assert.Equal(t, tt.format, got.TableFormat)
			assert.Equal(t, spi.SyncStatusError, got.StatusCode)
			assert.Equal(t, tt.wantErr, got.Error)
			assert.Equal(t, tt.duration, got.Duration)
			assert.Zero(t, got.LastInstantSynced, "a failed sync must not report a synced instant")
		})
	}
}
