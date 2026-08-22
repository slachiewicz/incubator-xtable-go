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

// Avro codec for Iceberg manifest lists and manifest files.
//
// The Iceberg specification mandates Avro object container files for both, and every engine that
// reads Iceberg — Spark, Trino, DuckDB, pyiceberg — assumes it. The field ids carried in the Avro
// schema as `field-id` properties are part of that contract: readers resolve columns by id, not by
// name, so a schema that loses them is unusable even though it parses.
//
// Records are written from and read into map[string]any rather than tagged structs. Writing needs
// it because the `partition` column's Avro type is per-table, derived from the partition spec, and
// a Go struct cannot express that. Reading needs it because the schema is whatever the engine that
// produced the file chose: v1 manifests carry no sequence numbers, writers differ over which
// optional statistic maps they emit, and hamba decodes against the schema embedded in the file.
// Pulling named fields out of a map tolerates all of that; a struct would match one writer's shape.
package iceberg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/iskorotkov/avro/v2"
	"github.com/iskorotkov/avro/v2/ocf"

	"github.com/slachiewicz/polytable/pkg/model"
)

// Manifest entry statuses, as defined by the Iceberg specification. Status 0, EXISTING, is the one
// this port never writes: it re-describes the whole file list on every commit.
const (
	manifestStatusAdded   = 1
	manifestStatusDeleted = 2
)

// contentData is the `content` value of a data file, as opposed to a delete file, and of a manifest
// holding data files.
const contentData = 0

// manifestEntrySchemaTemplate is the v2 manifest_entry schema with the per-table partition record
// left open. Everything else — names, field ids, and the key/value entry form the specification
// prescribes for the int-keyed statistic maps — is fixed.
const manifestEntrySchemaTemplate = `{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "field-id": 0, "type": "int"},
    {"name": "snapshot_id", "field-id": 1, "type": ["null", "long"], "default": null},
    {"name": "sequence_number", "field-id": 3, "type": ["null", "long"], "default": null},
    {"name": "file_sequence_number", "field-id": 4, "type": ["null", "long"], "default": null},
    {"name": "data_file", "field-id": 2, "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "field-id": 134, "type": "int"},
        {"name": "file_path", "field-id": 100, "type": "string"},
        {"name": "file_format", "field-id": 101, "type": "string"},
        {"name": "partition", "field-id": 102, "type": %s},
        {"name": "record_count", "field-id": 103, "type": "long"},
        {"name": "file_size_in_bytes", "field-id": 104, "type": "long"},
        {"name": "column_sizes", "field-id": 108, "default": null, "type": ["null",
          {"type": "array", "logicalType": "map", "items":
            {"type": "record", "name": "k117_v118", "fields": [
              {"name": "key", "field-id": 117, "type": "int"},
              {"name": "value", "field-id": 118, "type": "long"}]}}]},
        {"name": "value_counts", "field-id": 109, "default": null, "type": ["null",
          {"type": "array", "logicalType": "map", "items":
            {"type": "record", "name": "k119_v120", "fields": [
              {"name": "key", "field-id": 119, "type": "int"},
              {"name": "value", "field-id": 120, "type": "long"}]}}]},
        {"name": "null_value_counts", "field-id": 110, "default": null, "type": ["null",
          {"type": "array", "logicalType": "map", "items":
            {"type": "record", "name": "k121_v122", "fields": [
              {"name": "key", "field-id": 121, "type": "int"},
              {"name": "value", "field-id": 122, "type": "long"}]}}]},
        {"name": "nan_value_counts", "field-id": 137, "default": null, "type": ["null",
          {"type": "array", "logicalType": "map", "items":
            {"type": "record", "name": "k138_v139", "fields": [
              {"name": "key", "field-id": 138, "type": "int"},
              {"name": "value", "field-id": 139, "type": "long"}]}}]},
        {"name": "lower_bounds", "field-id": 125, "default": null, "type": ["null",
          {"type": "array", "logicalType": "map", "items":
            {"type": "record", "name": "k126_v127", "fields": [
              {"name": "key", "field-id": 126, "type": "int"},
              {"name": "value", "field-id": 127, "type": "bytes"}]}}]},
        {"name": "upper_bounds", "field-id": 128, "default": null, "type": ["null",
          {"type": "array", "logicalType": "map", "items":
            {"type": "record", "name": "k129_v130", "fields": [
              {"name": "key", "field-id": 129, "type": "int"},
              {"name": "value", "field-id": 130, "type": "bytes"}]}}]},
        {"name": "key_metadata", "field-id": 131, "default": null, "type": ["null", "bytes"]},
        {"name": "split_offsets", "field-id": 132, "default": null, "type": ["null",
          {"type": "array", "element-id": 133, "items": "long"}]},
        {"name": "equality_ids", "field-id": 135, "default": null, "type": ["null",
          {"type": "array", "element-id": 136, "items": "int"}]},
        {"name": "sort_order_id", "field-id": 140, "default": null, "type": ["null", "int"]}
      ]
    }}
  ]
}`

// manifestListSchema is the v2 manifest_file schema. Unlike a manifest it holds nothing
// table-specific, so it is a constant.
const manifestListSchema = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "field-id": 500, "type": "string"},
    {"name": "manifest_length", "field-id": 501, "type": "long"},
    {"name": "partition_spec_id", "field-id": 502, "type": "int"},
    {"name": "content", "field-id": 517, "type": "int"},
    {"name": "sequence_number", "field-id": 515, "type": "long"},
    {"name": "min_sequence_number", "field-id": 516, "type": "long"},
    {"name": "added_snapshot_id", "field-id": 503, "type": "long"},
    {"name": "added_files_count", "field-id": 504, "type": "int"},
    {"name": "existing_files_count", "field-id": 505, "type": "int"},
    {"name": "deleted_files_count", "field-id": 506, "type": "int"},
    {"name": "added_rows_count", "field-id": 512, "type": "long"},
    {"name": "existing_rows_count", "field-id": 513, "type": "long"},
    {"name": "deleted_rows_count", "field-id": 514, "type": "long"},
    {"name": "partitions", "field-id": 507, "default": null, "type": ["null",
      {"type": "array", "element-id": 508, "items":
        {"type": "record", "name": "r508", "fields": [
          {"name": "contains_null", "field-id": 509, "type": "boolean"},
          {"name": "contains_nan", "field-id": 518, "default": null, "type": ["null", "boolean"]},
          {"name": "lower_bound", "field-id": 510, "default": null, "type": ["null", "bytes"]},
          {"name": "upper_bound", "field-id": 511, "default": null, "type": ["null", "bytes"]}]}}]},
    {"name": "key_metadata", "field-id": 519, "default": null, "type": ["null", "bytes"]}
  ]
}`

// icebergFileFormats maps the canonical file format onto the name a manifest carries. The
// specification admits exactly these three spellings, and an engine rejects anything else outright:
// DuckDB answers `File format 'APACHE_PARQUET' not supported`, which is what writing the canonical
// name through produced before.
var icebergFileFormats = map[model.FileFormat]string{
	model.FileFormatParquet: "PARQUET",
	model.FileFormatORC:     "ORC",
	model.FileFormatAvro:    "AVRO",
}

// icebergFileFormat names a data file's format the way a manifest must spell it.
func icebergFileFormat(format model.FileFormat) (string, error) {
	name, ok := icebergFileFormats[format]
	if !ok {
		return "", fmt.Errorf("iceberg has no manifest spelling for the %s file format", format)
	}
	return name, nil
}

// modelFileFormat reverses icebergFileFormat. An unrecognized spelling falls back to Parquet, which
// is the only format this port reads: refusing the table would lose more than assuming its files
// are what every other part of the reader already assumes.
func modelFileFormat(name string) model.FileFormat {
	for format, spelling := range icebergFileFormats {
		if strings.EqualFold(name, spelling) {
			return format
		}
	}
	return model.FileFormatParquet
}

// partitionAvroTypes maps the Iceberg types this port can serialize as a partition value onto their
// Avro type. A type absent here is rejected at write time rather than written wrong: a partition
// value an engine misreads prunes away live rows, which is worse than a failed sync.
//
// Date and the timestamps are deliberately not here. Their Avro form carries a logical type whose
// unit this port does not yet normalize on either side of the conversion, and guessing the unit
// would put files in the wrong partition just as surely as the wrong type would.
var partitionAvroTypes = map[string]string{
	"boolean": "boolean",
	"int":     "int",
	"long":    "long",
	"float":   "float",
	"double":  "double",
	"string":  "string",
}

// partitionField is one column of a manifest's partition record, resolved to the Avro type its
// values must be coerced into.
type partitionField struct {
	name     string
	fieldID  int
	avroType string
}

// partitionEncoder turns the partition values of a data file into the Avro record a manifest
// declares. It is built once per manifest, from the partition spec and the schema its source ids
// point into, so that every entry is checked against the same resolved types.
type partitionEncoder struct {
	fields []partitionField
}

// newPartitionEncoder resolves the partition spec against the schema, failing on any transform or
// source type it cannot express.
func newPartitionEncoder(spec *PartitionSpec, tableSchema *TableSchema) (*partitionEncoder, error) {
	enc := &partitionEncoder{}
	if spec == nil {
		return enc, nil
	}

	for _, pf := range spec.Fields {
		if pf.Transform != "identity" {
			return nil, fmt.Errorf("iceberg partition field %s uses the %q transform, which this port "+
				"cannot write into a manifest", pf.Name, pf.Transform)
		}
		sourceType, err := partitionSourceType(pf, tableSchema)
		if err != nil {
			return nil, err
		}
		avroType, ok := partitionAvroTypes[sourceType]
		if !ok {
			return nil, fmt.Errorf("iceberg partition field %s has source type %s, which this port "+
				"cannot serialize into a manifest partition", pf.Name, sourceType)
		}
		enc.fields = append(enc.fields, partitionField{name: pf.Name, fieldID: pf.FieldID, avroType: avroType})
	}
	return enc, nil
}

// partitionSourceType names the Iceberg type of the column a partition field is derived from.
func partitionSourceType(pf *PartitionFieldDef, tableSchema *TableSchema) (string, error) {
	if tableSchema == nil {
		return "", fmt.Errorf("iceberg partition field %s cannot be typed without a schema", pf.Name)
	}
	for _, f := range tableSchema.Fields {
		if f.ID != pf.SourceID {
			continue
		}
		typeName, ok := f.Type.(string)
		if !ok {
			return "", fmt.Errorf("iceberg partition field %s is derived from the nested column %s, "+
				"which cannot be a partition source", pf.Name, f.Name)
		}
		return typeName, nil
	}
	return "", fmt.Errorf("iceberg partition field %s is derived from source id %d, which the schema "+
		"does not contain", pf.Name, pf.SourceID)
}

// schemaJSON renders the Avro record type of the `partition` column. Every field is nullable, which
// is the shape the Java implementation writes: a partition value legitimately can be null.
func (p *partitionEncoder) schemaJSON() (string, error) {
	fields := make([]map[string]any, 0, len(p.fields))
	for _, f := range p.fields {
		fields = append(fields, map[string]any{
			"name":     f.name,
			"field-id": f.fieldID,
			"type":     []any{"null", f.avroType},
			"default":  nil,
		})
	}
	encoded, err := json.Marshal(map[string]any{
		"type":   "record",
		"name":   "r102",
		"fields": fields,
	})
	if err != nil {
		return "", fmt.Errorf("failed to build the manifest partition schema: %w", err)
	}
	return string(encoded), nil
}

// record coerces the partition values of one data file into the declared types. A value that will
// not coerce is an error rather than a silent drop: the Hive-style sources hand back strings for
// every column, and a dropped value files the data under the wrong partition.
func (p *partitionEncoder) record(values map[string]any) (map[string]any, error) {
	record := make(map[string]any, len(p.fields))
	for _, f := range p.fields {
		raw, ok := values[f.name]
		if !ok || raw == nil {
			record[f.name] = nil
			continue
		}
		coerced, err := coercePartitionValue(f, raw)
		if err != nil {
			return nil, err
		}
		record[f.name] = coerced
	}
	return record, nil
}

// coercePartitionValue converts one partition value into the Go type its Avro type needs.
func coercePartitionValue(f partitionField, raw any) (any, error) {
	fail := func() (any, error) {
		return nil, fmt.Errorf("partition value %v (%T) for field %s is not a valid %s",
			raw, raw, f.name, f.avroType)
	}

	switch f.avroType {
	case "boolean":
		v, ok := coerceBool(raw)
		if !ok {
			return fail()
		}
		return v, nil
	case "int":
		v, ok := coerceInt64(raw)
		if !ok || v < math.MinInt32 || v > math.MaxInt32 {
			return fail()
		}
		return int32(v), nil //nolint:gosec // range-checked against int32 above
	case "long":
		v, ok := coerceInt64(raw)
		if !ok {
			return fail()
		}
		return v, nil
	case "float":
		v, ok := coerceFloat64(raw)
		if !ok {
			return fail()
		}
		return float32(v), nil
	case "double":
		v, ok := coerceFloat64(raw)
		if !ok {
			return fail()
		}
		return v, nil
	case "string":
		v, ok := coerceString(raw)
		if !ok {
			return fail()
		}
		return v, nil
	default:
		return fail()
	}
}

// writeManifest serializes manifest entries as an Avro manifest file.
//
// The key/value metadata is the set the specification requires: a reader takes the schema and the
// partition spec from the manifest itself rather than from the table metadata, because a manifest
// outlives the spec it was written under.
func writeManifest(entries []ManifestEntry, tableSchema *TableSchema, spec *PartitionSpec, formatVersion int) ([]byte, error) {
	if tableSchema == nil {
		return nil, fmt.Errorf("an iceberg manifest cannot be written without a schema")
	}

	partitions, err := newPartitionEncoder(spec, tableSchema)
	if err != nil {
		return nil, err
	}
	partitionType, err := partitions.schemaJSON()
	if err != nil {
		return nil, err
	}

	records := make([]map[string]any, 0, len(entries))
	for i := range entries {
		record, err := manifestEntryRecord(&entries[i], partitions)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	meta, err := manifestMetadata(tableSchema, spec, formatVersion)
	if err != nil {
		return nil, err
	}
	return writeAvroContainer(fmt.Sprintf(manifestEntrySchemaTemplate, partitionType), meta, records)
}

// manifestMetadata builds the key/value header of a manifest file.
func manifestMetadata(tableSchema *TableSchema, spec *PartitionSpec, formatVersion int) (map[string][]byte, error) {
	encodedSchema, err := json.Marshal(tableSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the iceberg schema for the manifest header: %w", err)
	}

	specFields := []*PartitionFieldDef{}
	specID := 0
	if spec != nil {
		specID = spec.SpecID
		if len(spec.Fields) > 0 {
			specFields = spec.Fields
		}
	}
	encodedSpec, err := json.Marshal(specFields)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the iceberg partition spec for the manifest header: %w", err)
	}

	return map[string][]byte{
		"schema":            encodedSchema,
		"schema-id":         fmt.Appendf(nil, "%d", tableSchema.SchemaID),
		"partition-spec":    encodedSpec,
		"partition-spec-id": fmt.Appendf(nil, "%d", specID),
		"format-version":    fmt.Appendf(nil, "%d", formatVersion),
		"content":           []byte("data"),
	}, nil
}

// manifestEntryRecord maps one manifest entry onto the Avro record shape. Every field of the schema
// is written, null where there is nothing to say: a map is resolved against the schema by name, and
// a missing key is not the same as an explicit null.
func manifestEntryRecord(entry *ManifestEntry, partitions *partitionEncoder) (map[string]any, error) {
	df := entry.DataFile
	if df == nil {
		return nil, fmt.Errorf("the manifest entry for snapshot %d carries no data file", entry.SnapshotID)
	}

	partition, err := partitions.record(df.Partition)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"status":               entry.Status,
		"snapshot_id":          entry.SnapshotID,
		"sequence_number":      optionalInt64(entry.SequenceNumber),
		"file_sequence_number": optionalInt64(entry.FileSequenceNumber),
		"data_file": map[string]any{
			"content":            df.Content,
			"file_path":          df.FilePath,
			"file_format":        df.FileFormat,
			"partition":          partition,
			"record_count":       df.RecordCount,
			"file_size_in_bytes": df.FileSizeInBytes,
			"column_sizes":       int64KVRecords(df.ColumnSizes),
			"value_counts":       int64KVRecords(df.ValueCounts),
			"null_value_counts":  int64KVRecords(df.NullValueCounts),
			"nan_value_counts":   int64KVRecords(df.NanValueCounts),
			"lower_bounds":       bytesKVRecords(df.LowerBounds),
			"upper_bounds":       bytesKVRecords(df.UpperBounds),
			"key_metadata":       nil,
			"split_offsets":      nil,
			"equality_ids":       nil,
			"sort_order_id":      nil,
		},
	}, nil
}

// writeManifestList serializes manifest list entries as an Avro manifest list file.
func writeManifestList(entries []ManifestListEntry, snapshotID int64, parentSnapshotID *int64, sequenceNumber int64, formatVersion int) ([]byte, error) {
	records := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		records = append(records, map[string]any{
			"manifest_path":        e.ManifestPath,
			"manifest_length":      e.ManifestLength,
			"partition_spec_id":    e.PartitionSpecID,
			"content":              e.Content,
			"sequence_number":      e.SequenceNumber,
			"min_sequence_number":  e.MinSequenceNumber,
			"added_snapshot_id":    e.AddedSnapshotID,
			"added_files_count":    e.AddedFilesCount,
			"existing_files_count": e.ExistingFilesCount,
			"deleted_files_count":  e.DeletedFilesCount,
			"added_rows_count":     e.AddedRowsCount,
			"existing_rows_count":  e.ExistingRowsCount,
			"deleted_rows_count":   e.DeletedRowsCount,
			// Per-partition summaries are optional and this port does not compute them; null says
			// "not recorded", which is what a reader must assume anyway.
			"partitions":   nil,
			"key_metadata": nil,
		})
	}

	parent := []byte("null")
	if parentSnapshotID != nil {
		parent = fmt.Appendf(nil, "%d", *parentSnapshotID)
	}
	meta := map[string][]byte{
		"snapshot-id":        fmt.Appendf(nil, "%d", snapshotID),
		"parent-snapshot-id": parent,
		"sequence-number":    fmt.Appendf(nil, "%d", sequenceNumber),
		"format-version":     fmt.Appendf(nil, "%d", formatVersion),
	}
	return writeAvroContainer(manifestListSchema, meta, records)
}

// readManifestList decodes an Avro manifest list.
func readManifestList(data []byte) ([]ManifestListEntry, error) {
	records, err := readAvroContainer(data)
	if err != nil {
		return nil, err
	}

	entries := make([]ManifestListEntry, 0, len(records))
	for _, r := range records {
		path := avroString(r, "manifest_path")
		if path == "" {
			return nil, fmt.Errorf("a manifest list entry carries no manifest path")
		}
		entries = append(entries, ManifestListEntry{
			ManifestPath:       path,
			ManifestLength:     avroInt64(r, "manifest_length"),
			PartitionSpecID:    avroInt(r, "partition_spec_id"),
			Content:            avroInt(r, "content"),
			SequenceNumber:     avroInt64(r, "sequence_number"),
			MinSequenceNumber:  avroInt64(r, "min_sequence_number"),
			AddedSnapshotID:    avroInt64(r, "added_snapshot_id"),
			AddedFilesCount:    avroInt(r, "added_files_count"),
			ExistingFilesCount: avroInt(r, "existing_files_count"),
			DeletedFilesCount:  avroInt(r, "deleted_files_count"),
			AddedRowsCount:     avroInt64(r, "added_rows_count"),
			ExistingRowsCount:  avroInt64(r, "existing_rows_count"),
			DeletedRowsCount:   avroInt64(r, "deleted_rows_count"),
		})
	}
	return entries, nil
}

// readManifest decodes an Avro manifest file.
func readManifest(data []byte) ([]ManifestEntry, error) {
	records, err := readAvroContainer(data)
	if err != nil {
		return nil, err
	}

	entries := make([]ManifestEntry, 0, len(records))
	for _, r := range records {
		dataFile := avroRecord(r, "data_file")
		if dataFile == nil {
			return nil, fmt.Errorf("a manifest entry carries no data file")
		}
		entries = append(entries, ManifestEntry{
			Status:             avroInt(r, "status"),
			SnapshotID:         avroInt64(r, "snapshot_id"),
			SequenceNumber:     avroOptionalInt64(r, "sequence_number"),
			FileSequenceNumber: avroOptionalInt64(r, "file_sequence_number"),
			DataFile: &ManifestDataFile{
				Content:         avroInt(dataFile, "content"),
				FilePath:        avroString(dataFile, "file_path"),
				FileFormat:      avroString(dataFile, "file_format"),
				Partition:       avroRecord(dataFile, "partition"),
				RecordCount:     avroInt64(dataFile, "record_count"),
				FileSizeInBytes: avroInt64(dataFile, "file_size_in_bytes"),
				ColumnSizes:     avroKVInt64(dataFile, "column_sizes"),
				ValueCounts:     avroKVInt64(dataFile, "value_counts"),
				NullValueCounts: avroKVInt64(dataFile, "null_value_counts"),
				NanValueCounts:  avroKVInt64(dataFile, "nan_value_counts"),
				LowerBounds:     avroKVBytes(dataFile, "lower_bounds"),
				UpperBounds:     avroKVBytes(dataFile, "upper_bounds"),
			},
		})
	}
	return entries, nil
}

// writeAvroContainer encodes records into an Avro object container file.
//
// The schema is written through a marshaler that hands back the literal JSON rather than
// re-serializing the parsed schema, because a round trip through the parser is not guaranteed to
// preserve the `field-id` properties — and a manifest without them is one no engine can resolve
// columns against.
func writeAvroContainer(schemaJSON string, meta map[string][]byte, records []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schemaJSON, &buf,
		ocf.WithCodec(ocf.Deflate),
		ocf.WithMetadata(meta),
		ocf.WithSchemaMarshaler(func(avro.Schema) ([]byte, error) { return []byte(schemaJSON), nil }),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open the avro writer: %w", err)
	}
	for _, record := range records {
		if err := enc.Encode(record); err != nil {
			_ = enc.Close()
			return nil, fmt.Errorf("failed to encode an avro record: %w", err)
		}
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("failed to finish the avro container: %w", err)
	}
	return buf.Bytes(), nil
}

// readAvroContainer decodes every record of an Avro object container file against the schema the
// file itself carries.
func readAvroContainer(data []byte) ([]map[string]any, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open the avro container: %w", err)
	}

	var records []map[string]any
	for dec.HasNext() {
		record := make(map[string]any)
		if err := dec.Decode(&record); err != nil {
			return nil, fmt.Errorf("failed to decode an avro record: %w", err)
		}
		records = append(records, record)
	}
	if err := dec.Error(); err != nil {
		return nil, fmt.Errorf("failed to read the avro container: %w", err)
	}
	return records, nil
}

// optionalInt64 turns a nil pointer into an explicit Avro null.
func optionalInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

// int64KVRecords renders an int-keyed map as the key/value entry array the specification
// prescribes: an Avro map takes string keys only, so this array form is the only encoding available
// for a map keyed by field id. Entries are ordered by key so that a manifest is reproducible.
func int64KVRecords(m map[int]int64) any {
	if len(m) == 0 {
		return nil
	}
	entries := make([]any, 0, len(m))
	for _, key := range slices.Sorted(maps.Keys(m)) {
		entries = append(entries, map[string]any{"key": key, "value": m[key]})
	}
	return entries
}

// bytesKVRecords is int64KVRecords for the bound maps, whose values are the specification's
// single-value binary serialization.
func bytesKVRecords(m map[int][]byte) any {
	if len(m) == 0 {
		return nil
	}
	entries := make([]any, 0, len(m))
	for _, key := range slices.Sorted(maps.Keys(m)) {
		entries = append(entries, map[string]any{"key": key, "value": m[key]})
	}
	return entries
}

// avroString reads a string field, returning "" when it is absent or null.
func avroString(record map[string]any, key string) string {
	s, ok := coerceString(record[key])
	if !ok {
		return ""
	}
	return s
}

// avroInt64 reads an integral field, returning 0 when it is absent or null. Which Go type a number
// arrives as depends on the writer's schema — an Avro int decodes to int and a long to int64, and
// the same Iceberg field is one or the other across format versions — so the coercion is not
// defensive.
func avroInt64(record map[string]any, key string) int64 {
	n, ok := coerceInt64(record[key])
	if !ok {
		return 0
	}
	return n
}

// avroInt is avroInt64 for a field that is an Avro int.
func avroInt(record map[string]any, key string) int {
	return int(avroInt64(record, key))
}

// avroOptionalInt64 distinguishes an absent or null field from a zero one.
func avroOptionalInt64(record map[string]any, key string) *int64 {
	raw := record[key]
	if raw == nil {
		return nil
	}
	n, ok := coerceInt64(raw)
	if !ok {
		return nil
	}
	return &n
}

// avroRecord reads a nested record field as a map.
func avroRecord(record map[string]any, key string) map[string]any {
	nested, _ := record[key].(map[string]any)
	return nested
}

// avroArray reads an array field, seeing through the union envelope. hamba decodes the selected
// branch of a union over complex types into a single-entry map keyed by the branch's type name, so
// a nullable array arrives as {"array": [...]}.
func avroArray(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	case map[string]any:
		if inner, ok := typed["array"]; ok {
			return avroArray(inner)
		}
	}
	return nil
}

// avroKVInt64 reads one of the key/value entry arrays back into an int-keyed map.
func avroKVInt64(record map[string]any, key string) map[int]int64 {
	entries := avroArray(record[key])
	if len(entries) == 0 {
		return nil
	}

	out := make(map[int]int64, len(entries))
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, ok := coerceInt64(entry["key"])
		if !ok {
			continue
		}
		value, ok := coerceInt64(entry["value"])
		if !ok {
			continue
		}
		out[int(id)] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// avroKVBytes is avroKVInt64 for the bound maps.
func avroKVBytes(record map[string]any, key string) map[int][]byte {
	entries := avroArray(record[key])
	if len(entries) == 0 {
		return nil
	}

	out := make(map[int][]byte, len(entries))
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, ok := coerceInt64(entry["key"])
		if !ok {
			continue
		}
		value, ok := entry["value"].([]byte)
		if !ok {
			continue
		}
		out[int(id)] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
