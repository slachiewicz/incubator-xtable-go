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

import "fmt"

// Type represents the canonical data type in XTable's internal type system.
type Type string

// Canonical data types.
const (
	TypeRecord       Type = "RECORD"
	TypeEnum         Type = "ENUM"
	TypeList         Type = "LIST"
	TypeMap          Type = "MAP"
	TypeUnion        Type = "UNION"
	TypeUUID         Type = "UUID"
	TypeFixed        Type = "FIXED"
	TypeString       Type = "STRING"
	TypeBytes        Type = "BYTES"
	TypeInt          Type = "INT"
	TypeLong         Type = "LONG"
	TypeFloat        Type = "FLOAT"
	TypeDouble       Type = "DOUBLE"
	TypeBoolean      Type = "BOOLEAN"
	TypeNull         Type = "NULL"
	TypeDate         Type = "DATE"
	TypeDecimal      Type = "DECIMAL"
	TypeTimestamp    Type = "TIMESTAMP"
	TypeTimestampNTZ Type = "TIMESTAMP_NTZ"
)

// String returns the string representation of Type.
func (t Type) String() string {
	return string(t)
}

// IsNonScalar returns true if the type is a composite/nested type.
func (t Type) IsNonScalar() bool {
	switch t {
	case TypeRecord, TypeList, TypeMap, TypeUnion:
		return true
	default:
		return false
	}
}

// MetadataKey represents keys for type-specific metadata (e.g. decimal precision, time unit).
type MetadataKey string

// Type-specific metadata keys.
const (
	MetadataKeyDecimalScale       MetadataKey = "DECIMAL_SCALE"
	MetadataKeyDecimalPrecision   MetadataKey = "DECIMAL_PRECISION"
	MetadataKeyEnumValues         MetadataKey = "ENUM_VALUES"
	MetadataKeyFixedBytesSize     MetadataKey = "FIXED_BYTES_SIZE"
	MetadataKeyTimestampPrecision MetadataKey = "TIMESTAMP_PRECISION"
)

// MetadataValue represents values for metadata configurations.
type MetadataValue string

// Time-unit metadata values.
const (
	MetadataValueMicros MetadataValue = "MICROS"
	MetadataValueMillis MetadataValue = "MILLIS"
	MetadataValueNanos  MetadataValue = "NANOS"
)

// FileFormat represents the physical data file format.
type FileFormat string

// Supported physical data file formats.
const (
	FileFormatParquet FileFormat = "APACHE_PARQUET"
	FileFormatORC     FileFormat = "APACHE_ORC"
	FileFormatAvro    FileFormat = "APACHE_AVRO"
)

// TableFormat represents standard lakehouse table format identifiers.
type TableFormat string

// Supported lakehouse table formats.
const (
	TableFormatHudi    TableFormat = "HUDI"
	TableFormatIceberg TableFormat = "ICEBERG"
	TableFormatDelta   TableFormat = "DELTA"
	TableFormatPaimon  TableFormat = "PAIMON"
	TableFormatParquet TableFormat = "PARQUET"
)

// SupportedTableFormats returns the list of all supported table formats.
func SupportedTableFormats() []TableFormat {
	return []TableFormat{
		TableFormatHudi,
		TableFormatIceberg,
		TableFormatDelta,
		TableFormatPaimon,
		TableFormatParquet,
	}
}

// ParseTableFormat converts a string to TableFormat.
func ParseTableFormat(s string) (TableFormat, error) {
	switch s {
	case "HUDI", "hudi":
		return TableFormatHudi, nil
	case "ICEBERG", "iceberg":
		return TableFormatIceberg, nil
	case "DELTA", "delta":
		return TableFormatDelta, nil
	case "PAIMON", "paimon":
		return TableFormatPaimon, nil
	case "PARQUET", "parquet":
		return TableFormatParquet, nil
	default:
		return "", fmt.Errorf("unknown table format: %s", s)
	}
}
