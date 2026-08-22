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

package model_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/model"
)

// javaSample is the exact XTABLE_METADATA value Java XTable wrote when it synced Delta to Iceberg,
// captured in T60 (docs/improvement-plan.md) from a real table.
const javaSample = `{"lastInstantSynced":"2026-08-22T16:10:52Z","instantsToConsiderForNextSync":[],"version":0,"sourceTableFormat":"DELTA","sourceIdentifier":"1"}`

// javaSampleInstantMillis is javaSample's lastInstantSynced ("2026-08-22T16:10:52Z") expressed as
// epoch milliseconds, computed independently of the code under test.
const javaSampleInstantMillis int64 = 1787415052000

func TestParseXTableMetadataJSON_RealJavaSample(t *testing.T) {
	t.Parallel()

	meta, err := model.ParseXTableMetadataJSON(javaSample)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, javaSampleInstantMillis, meta.LastInstantSynced)
	assert.Equal(t, model.TableFormat("DELTA"), meta.SourceFormat)
	assert.Equal(t, "1", meta.SourceIdentifier)
	assert.Empty(t, meta.InstantsToConsiderForNextSync)
}

func TestParseXTableMetadataJSON_RejectsUnsupportedVersion(t *testing.T) {
	t.Parallel()

	_, err := model.ParseXTableMetadataJSON(
		`{"lastInstantSynced":"2026-08-22T16:10:52Z","version":1,"sourceTableFormat":"DELTA"}`)
	require.Error(t, err)
}

// TestReadSyncMetadataFromProperties_JavaFixture replays a real Java-XTable-synced Iceberg table's
// property map, captured from abfss://xtable-pr897@polytable410464.dfs.core.windows.net/
// src/delta_small/metadata/v3.metadata.json (T60, docs/improvement-plan.md) rather than a synthetic
// map, since a fixture drawn from an actual table is more likely to catch a shape mismatch than a
// case invented from the spec alone.
func TestReadSyncMetadataFromProperties_JavaFixture(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/java_xtable_iceberg_properties.json")
	require.NoError(t, err)

	var props map[string]string
	require.NoError(t, json.Unmarshal(data, &props))

	meta := model.ReadSyncMetadataFromProperties(props)
	require.NotNil(t, meta)
	assert.Equal(t, javaSampleInstantMillis, meta.LastInstantSynced)
	assert.Equal(t, model.TableFormat("DELTA"), meta.SourceFormat)
	assert.Equal(t, "1", meta.SourceIdentifier)
}

func TestReadSyncMetadataFromProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		props      map[string]string
		wantNil    bool
		wantMillis int64
		wantFormat model.TableFormat
		wantSrcID  string
	}{
		{
			name:    "no properties returns nil",
			props:   nil,
			wantNil: true,
		},
		{
			name: "only XTABLE_METADATA is recognized",
			props: map[string]string{
				model.KeyXTableMetadata: javaSample,
			},
			wantMillis: javaSampleInstantMillis,
			wantFormat: "DELTA",
			wantSrcID:  "1",
		},
		{
			// Regression guard: a table written before this change, or by a polytable build that
			// only ever wrote the flat keys, must still be recognized exactly as before.
			name: "only flat keys still works (regression guard)",
			props: map[string]string{
				model.KeyLastInstantSynced: "1700000000000",
				model.KeySourceFormat:      "DELTA",
			},
			wantMillis: 1700000000000,
			wantFormat: "DELTA",
		},
		{
			// Both present prefers XTABLE_METADATA: it is the richer, canonical shape (it alone
			// carries sourceIdentifier and pending instants) and, after this change, every
			// polytable target writes both on every sync, so for a table only polytable has
			// touched the two never disagree. Here they deliberately disagree so the test can tell
			// which one won.
			name: "both present prefers XTABLE_METADATA because it is the richer canonical shape",
			props: map[string]string{
				model.KeyXTableMetadata:    javaSample,
				model.KeyLastInstantSynced: "1",
				model.KeySourceFormat:      "ICEBERG",
			},
			wantMillis: javaSampleInstantMillis,
			wantFormat: "DELTA",
			wantSrcID:  "1",
		},
		{
			// Malformed XTABLE_METADATA (not JSON at all) falls back to the flat keys rather than
			// failing the read: a corrupted copy of the optional, richer property should not be
			// worse than never having written it.
			name: "malformed XTABLE_METADATA falls back to flat keys when present",
			props: map[string]string{
				model.KeyXTableMetadata:    "{not json",
				model.KeyLastInstantSynced: "1700000000000",
				model.KeySourceFormat:      "DELTA",
			},
			wantMillis: 1700000000000,
			wantFormat: "DELTA",
		},
		{
			// Malformed XTABLE_METADATA with no flat keys to fall back to must not panic, and must
			// not manufacture a zero-instant metadata that a caller could mistake for a real (if
			// very old) prior sync: it returns nil, the same "no prior sync" state as a table that
			// was never synced at all.
			name: "malformed XTABLE_METADATA with no flat keys returns nil, not a zero instant",
			props: map[string]string{
				model.KeyXTableMetadata: "{not json",
			},
			wantNil: true,
		},
		{
			// XTABLE_METADATA missing its one required field (Java's own fromJson rejects this the
			// same way) falls back exactly like non-JSON garbage would.
			name: "XTABLE_METADATA missing lastInstantSynced falls back to flat keys",
			props: map[string]string{
				model.KeyXTableMetadata:    `{"sourceTableFormat":"DELTA"}`,
				model.KeyLastInstantSynced: "42",
			},
			wantMillis: 42,
		},
		{
			// A version newer than this code understands may have changed field semantics, so it is
			// rejected the same way Java's own fromJson rejects it, and falls back like any other
			// malformed XTABLE_METADATA.
			name: "XTABLE_METADATA with an unsupported version falls back to flat keys",
			props: map[string]string{
				model.KeyXTableMetadata:    `{"lastInstantSynced":"2026-08-22T16:10:52Z","version":1,"sourceTableFormat":"DELTA"}`,
				model.KeyLastInstantSynced: "42",
			},
			wantMillis: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meta := model.ReadSyncMetadataFromProperties(tt.props)
			if tt.wantNil {
				assert.Nil(t, meta)
				return
			}
			require.NotNil(t, meta)
			assert.Equal(t, tt.wantMillis, meta.LastInstantSynced)
			if tt.wantFormat != "" {
				assert.Equal(t, tt.wantFormat, meta.SourceFormat)
			}
			if tt.wantSrcID != "" {
				assert.Equal(t, tt.wantSrcID, meta.SourceIdentifier)
			}
		})
	}
}

// TestWriteSyncMetadataProperties_WritesBothShapes is the write-side counterpart to
// TestReadSyncMetadataFromProperties: every sync must leave both polytable's own flat keys and
// Java's XTABLE_METADATA behind, or one direction of interop silently regresses.
func TestWriteSyncMetadataProperties_WritesBothShapes(t *testing.T) {
	t.Parallel()

	meta := &model.TableSyncMetadata{
		LastInstantSynced: javaSampleInstantMillis,
		SourceFormat:      "DELTA",
		SourceIdentifier:  "1",
	}
	props := make(map[string]string)
	model.WriteSyncMetadataProperties(props, meta)

	require.Contains(t, props, model.KeyLastInstantSynced)
	require.Contains(t, props, model.KeySourceFormat)
	require.Contains(t, props, model.KeyXTableMetadata)
	assert.Equal(t, "DELTA", props[model.KeySourceFormat])

	parsed, err := model.ParseXTableMetadataJSON(props[model.KeyXTableMetadata])
	require.NoError(t, err)
	assert.Equal(t, javaSampleInstantMillis, parsed.LastInstantSynced)
	assert.Equal(t, model.TableFormat("DELTA"), parsed.SourceFormat)
	assert.Equal(t, "1", parsed.SourceIdentifier)
}

// TestSyncMetadataInstantRoundTrip_NoDrift exercises the ISO-8601-versus-epoch-millis conversion
// through the public write/read path: a millisecond-precision instant must come back identical, not
// merely close, since a lost millisecond in a sync watermark either re-syncs a commit or skips one.
func TestSyncMetadataInstantRoundTrip_NoDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		millis int64
	}{
		{name: "whole second, matches the real Java sample shape", millis: javaSampleInstantMillis},
		{name: "zero", millis: 0},
		{name: "sub-second millis component", millis: 1700000000123},
		{name: "millis component at the low end", millis: 1700000000001},
		{name: "far future instant", millis: 4102444800042},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			props := make(map[string]string)
			model.WriteSyncMetadataProperties(props, &model.TableSyncMetadata{LastInstantSynced: tt.millis})

			meta := model.ReadSyncMetadataFromProperties(props)
			require.NotNil(t, meta)
			assert.Equal(t, tt.millis, meta.LastInstantSynced)

			parsed, err := model.ParseXTableMetadataJSON(props[model.KeyXTableMetadata])
			require.NoError(t, err)
			assert.Equal(t, tt.millis, parsed.LastInstantSynced)
		})
	}
}
