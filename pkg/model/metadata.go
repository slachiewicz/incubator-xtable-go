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

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

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
	// SourceIdentifier is the source-side commit version/instant this sync state reflects (e.g. a
	// Delta version, a Hudi instant, an Iceberg snapshot id). It mirrors Java XTable's
	// TableSyncMetadata.sourceIdentifier field and model.Snapshot/model.TableChange's own
	// SourceIdentifier -- the same value, just carried into the sync watermark as well.
	SourceIdentifier string `json:"sourceIdentifier,omitempty"`
	// CustomProperties stores additional provider-specific sync attributes.
	CustomProperties map[string]string `json:"customProperties,omitempty"`
}

const (
	// MetadataPropertyPrefix is the key prefix for polytable's own flat sync-metadata table
	// properties. This is NOT the property Java XTable reads or writes -- see KeyXTableMetadata for
	// that. Kept for tables only polytable has touched and for backward compatibility with earlier
	// polytable versions; every target now writes both shapes (T60, docs/improvement-plan.md).
	MetadataPropertyPrefix = "xtable_"
	// KeyLastInstantSynced is the property key for the last synced instant (polytable's own flat
	// shape: an epoch-millisecond string).
	KeyLastInstantSynced = "xtable_last_instant_synced"
	// KeySourceFormat is the property key for the source table format (polytable's own flat shape).
	KeySourceFormat = "xtable_source_format"

	// KeyXTableMetadata is the table property key Apache XTable (Java) reads and writes:
	// org.apache.xtable.model.metadata.TableSyncMetadata.XTABLE_METADATA. Unlike polytable's flat
	// keys, Java stores the entire sync state as one JSON blob under this single property name, with
	// an ISO-8601 instant rather than epoch millis. Confirmed against a real Java-XTable-synced
	// Iceberg table (T60): {"lastInstantSynced":"2026-08-22T16:10:52Z",
	// "instantsToConsiderForNextSync":[],"version":0,"sourceTableFormat":"DELTA",
	// "sourceIdentifier":"1"}.
	KeyXTableMetadata = "XTABLE_METADATA"
)

// xtableMetadataVersion is the "version" field polytable writes into KeyXTableMetadata. Java's
// TableSyncMetadata.CURRENT_VERSION is 0 in the release this was verified against, and Java's own
// fromJson refuses to read a version greater than the one it knows how to handle. Do not bump this
// without checking that upstream Java XTable has moved too.
const xtableMetadataVersion = 0

// javaSyncMetadataJSON mirrors org.apache.xtable.model.metadata.TableSyncMetadata's Jackson shape
// field-for-field: same names, and instantsToConsiderForNextSync is never omitted even when empty.
// Java's own incremental-sync filter (org.apache.xtable.spi.sync.TableFormatSync,
// isChangeApplicableForLastSyncMetadata) calls .contains(...) on that list unconditionally, so a
// property written without it (or with it null) would NPE a Java reader -- it must always be
// present, as "[]" at minimum.
type javaSyncMetadataJSON struct {
	LastInstantSynced             string   `json:"lastInstantSynced"`
	InstantsToConsiderForNextSync []string `json:"instantsToConsiderForNextSync"`
	Version                       int      `json:"version"`
	SourceTableFormat             string   `json:"sourceTableFormat,omitempty"`
	SourceIdentifier              string   `json:"sourceIdentifier,omitempty"`
}

// instantToISO8601 renders an epoch-millisecond instant the way Java's Jackson
// (JavaTimeModule + SerializationFeature.WRITE_DATES_AS_TIMESTAMPS=false) renders a java.time.Instant:
// whole seconds with no fractional part when the millisecond component is zero, or the fractional
// seconds otherwise. java.time.format.DateTimeFormatterBuilder#appendInstant(-1), which is what
// Jackson's InstantSerializer uses, picks the fractional-digit count from the nanosecond value: 0
// digits when it is zero, else the smallest of 3/6/9 digits that represents it exactly -- and since
// polytable's instants only ever carry millisecond precision, that is always either 0 or 3 digits.
// Go's time.RFC3339Nano, driven by a millisecond-only nanosecond value, produces the identical
// decimal digits (its "trim trailing zeros" rule and Java's "smallest exact group" rule agree
// whenever the value is a whole number of milliseconds), so this needs no bespoke formatter.
func instantToISO8601(epochMillis int64) string {
	return time.UnixMilli(epochMillis).UTC().Format(time.RFC3339Nano)
}

// iso8601ToInstant parses an ISO-8601 instant, with or without a fractional-second component, back
// into epoch milliseconds. It rejects an instant with sub-millisecond precision it cannot represent
// exactly rather than truncating it silently: T60 requires this conversion not to drift, and a
// truncated sync watermark either replays a commit or skips one.
func iso8601ToInstant(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("invalid ISO-8601 instant %q: %w", s, err)
	}
	if t.Nanosecond()%int(time.Millisecond) != 0 {
		return 0, fmt.Errorf("instant %q has sub-millisecond precision polytable cannot represent losslessly", s)
	}
	return t.UnixMilli(), nil
}

// xtableMetadataJSON renders meta into the JSON blob Java XTable writes under KeyXTableMetadata.
func xtableMetadataJSON(meta *TableSyncMetadata) (string, error) {
	pending := make([]string, len(meta.InstantsToConsiderForNextSync))
	for i, ms := range meta.InstantsToConsiderForNextSync {
		pending[i] = instantToISO8601(ms)
	}
	raw := javaSyncMetadataJSON{
		LastInstantSynced:             instantToISO8601(meta.LastInstantSynced),
		InstantsToConsiderForNextSync: pending,
		Version:                       xtableMetadataVersion,
		SourceTableFormat:             string(meta.SourceFormat),
		SourceIdentifier:              meta.SourceIdentifier,
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return "", fmt.Errorf("failed to marshal XTABLE_METADATA: %w", err)
	}
	return string(data), nil
}

// ParseXTableMetadataJSON parses the JSON blob Java XTable writes under KeyXTableMetadata into a
// TableSyncMetadata. It mirrors Java's own TableSyncMetadata.fromJson: lastInstantSynced is
// required (Java throws ParseException without it), everything else is optional. TargetFormat and
// CustomProperties are not part of Java's payload and are left unset; the caller fills them in from
// context the way every GetTableMetadata implementation already does for the flat keys.
func ParseXTableMetadataJSON(data string) (*TableSyncMetadata, error) {
	var raw javaSyncMetadataJSON
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return nil, fmt.Errorf("invalid XTABLE_METADATA JSON: %w", err)
	}
	if raw.LastInstantSynced == "" {
		return nil, fmt.Errorf("XTABLE_METADATA is missing required field lastInstantSynced")
	}
	if raw.Version > xtableMetadataVersion {
		// Mirrors Java's own TableSyncMetadata.fromJson: a version newer than the one this code
		// knows how to handle may have changed field semantics, so trusting its instant would be a
		// guess. ReadSyncMetadataFromProperties falls back to the flat keys, or to "no metadata", on
		// this error exactly as it does for malformed JSON.
		return nil, fmt.Errorf("XTABLE_METADATA version %d is newer than the supported version %d", raw.Version, xtableMetadataVersion)
	}
	lastInstant, err := iso8601ToInstant(raw.LastInstantSynced)
	if err != nil {
		return nil, fmt.Errorf("XTABLE_METADATA lastInstantSynced: %w", err)
	}

	meta := &TableSyncMetadata{
		LastInstantSynced: lastInstant,
		SourceFormat:      TableFormat(raw.SourceTableFormat),
		SourceIdentifier:  raw.SourceIdentifier,
	}
	for _, s := range raw.InstantsToConsiderForNextSync {
		instant, err := iso8601ToInstant(s)
		if err != nil {
			return nil, fmt.Errorf("XTABLE_METADATA instantsToConsiderForNextSync: %w", err)
		}
		meta.InstantsToConsiderForNextSync = append(meta.InstantsToConsiderForNextSync, instant)
	}
	return meta, nil
}

// ReadSyncMetadataFromProperties recovers a TableSyncMetadata from a target's table properties,
// recognizing both shapes a sync may have left behind: Java XTable's single KeyXTableMetadata JSON
// blob, and polytable's own flat KeyLastInstantSynced/KeySourceFormat keys. It returns nil when
// neither is present or usable -- callers already treat a nil TableSyncMetadata as "no prior sync",
// which is always the safe reading (pkg/conversion/controller.go additionally refuses to trust a
// non-positive LastInstantSynced for an incremental sync, so this can never manufacture a
// zero-instant incremental resume).
//
// When both are present, KeyXTableMetadata wins: it is the richer, canonical shape (it alone
// carries SourceIdentifier and pending instants), and after this change every polytable target
// writes both on every sync, so the two never disagree for a table only polytable has touched. If
// KeyXTableMetadata is present but malformed, this falls back to the flat keys rather than failing
// the read outright -- a corrupted copy of the optional, richer property should not be worse than
// never having written it, and the flat keys, when present, are still a valid record of the last
// synced instant.
func ReadSyncMetadataFromProperties(props map[string]string) *TableSyncMetadata {
	if len(props) == 0 {
		return nil
	}

	if blob, ok := props[KeyXTableMetadata]; ok && blob != "" {
		if meta, err := ParseXTableMetadataJSON(blob); err == nil {
			return meta
		}
	}

	lastInstantStr, ok := props[KeyLastInstantSynced]
	if !ok {
		return nil
	}
	lastInstant, err := strconv.ParseInt(lastInstantStr, 10, 64)
	if err != nil {
		return nil
	}
	meta := &TableSyncMetadata{LastInstantSynced: lastInstant}
	if srcFormat, ok := props[KeySourceFormat]; ok {
		meta.SourceFormat = TableFormat(srcFormat)
	}
	return meta
}

// WriteSyncMetadataProperties sets the sync-metadata table properties for meta on props, mutating
// it in place. It deliberately writes two overlapping representations of the same state:
// polytable's own flat keys (KeyLastInstantSynced, KeySourceFormat), which every target's
// GetTableMetadata falls back to, and Java XTable's single KeyXTableMetadata JSON property, which
// is what upstream Java XTable -- and Microsoft Fabric, which re-exposes Delta as Iceberg through
// that same library -- reads and writes.
//
// Writing only the flat keys means Java (and Fabric) never recognizes a polytable-synced table and
// resyncs it in full; writing only KeyXTableMetadata does the same to polytable itself on an older
// version, or to any other reader still expecting the flat keys. The second property is the entire
// cost of interoperating both ways, so both are always written. Do not delete either without first
// confirming every consumer this repository cannot see has moved off it -- T60 in
// docs/improvement-plan.md has the evidence this was a real, silent double resync.
func WriteSyncMetadataProperties(props map[string]string, meta *TableSyncMetadata) {
	props[KeyLastInstantSynced] = strconv.FormatInt(meta.LastInstantSynced, 10)
	if meta.SourceFormat != "" {
		props[KeySourceFormat] = string(meta.SourceFormat)
	}
	if blob, err := xtableMetadataJSON(meta); err == nil {
		props[KeyXTableMetadata] = blob
	}
}
