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

import "strings"

// PartitionTransformType represents the transform applied on a source column to produce partition values.
type PartitionTransformType string

// Supported partition transforms.
const (
	PartitionTransformValue PartitionTransformType = "VALUE"
	PartitionTransformYear  PartitionTransformType = "YEAR"
	PartitionTransformMonth PartitionTransformType = "MONTH"
	PartitionTransformDay   PartitionTransformType = "DAY"
	PartitionTransformHour  PartitionTransformType = "HOUR"
)

// PartitionField represents a partition field specification.
type PartitionField struct {
	// SourceField is the reference to the table field used for partitioning.
	SourceField *Field `json:"sourceField"`
	// TransformType describes how the partition value is generated from the field value.
	TransformType PartitionTransformType `json:"transformType"`
	// Format is an optional date/time pattern (e.g., yyyy-MM-dd) when transform type is temporal.
	Format string `json:"format,omitempty"`
	// CustomPartitionName is an optional custom partition column name.
	CustomPartitionName string `json:"customPartitionName,omitempty"`
}

// Field represents a single named field in an internal schema hierarchy.
type Field struct {
	// Name is the field identifier.
	Name string `json:"name"`
	// ParentPath is the dot-delimited path to the parent (empty for top-level fields).
	ParentPath string `json:"parentPath,omitempty"`
	// Schema is the data type and nested structure of this field.
	Schema *Schema `json:"schema"`
	// FieldID is the optional unique integer field identifier (critical for Iceberg).
	FieldID *int `json:"fieldId,omitempty"`
	// DefaultValue represents a default value if defined.
	DefaultValue any `json:"defaultValue,omitempty"`
}

// Path returns the full dot-delimited path of the field (e.g. "address.city").
func (f *Field) Path() string {
	if f.ParentPath == "" {
		return f.Name
	}
	return f.ParentPath + "." + f.Name
}

// Schema represents a type definition in XTable's internal schema model.
type Schema struct {
	// Name of this schema definition (optional for anonymous types).
	Name string `json:"name,omitempty"`
	// DataType is the canonical data type.
	DataType Type `json:"dataType"`
	// Comment is a user-readable description of this field/schema.
	Comment string `json:"comment,omitempty"`
	// IsNullable indicates if values of this field can be null.
	IsNullable bool `json:"isNullable"`
	// Fields contains the list of child fields for RECORD / STRUCT types.
	Fields []*Field `json:"fields,omitempty"`
	// ElementSchema contains the item schema when DataType is LIST.
	ElementSchema *Field `json:"elementSchema,omitempty"`
	// KeySchema and ValueSchema contain key and value field schemas when DataType is MAP.
	KeySchema   *Field `json:"keySchema,omitempty"`
	ValueSchema *Field `json:"valueSchema,omitempty"`
	// RecordKeyFields lists the primary key / record key fields for the table.
	RecordKeyFields []*Field `json:"recordKeyFields,omitempty"`
	// Metadata holds type-specific metadata (e.g. DECIMAL_PRECISION, DECIMAL_SCALE).
	Metadata map[MetadataKey]any `json:"metadata,omitempty"`
}

// NewPrimitiveSchema creates an internal schema for a primitive scalar type.
func NewPrimitiveSchema(t Type, nullable bool) *Schema {
	return &Schema{
		DataType:   t,
		IsNullable: nullable,
		Metadata:   make(map[MetadataKey]any),
	}
}

// NewDecimalSchema creates an internal schema for a decimal type with precision and scale.
func NewDecimalSchema(precision, scale int, nullable bool) *Schema {
	return &Schema{
		DataType:   TypeDecimal,
		IsNullable: nullable,
		Metadata: map[MetadataKey]any{
			MetadataKeyDecimalPrecision: precision,
			MetadataKeyDecimalScale:     scale,
		},
	}
}

// NewRecordSchema creates a composite struct/record schema with child fields.
func NewRecordSchema(name string, fields []*Field, nullable bool) *Schema {
	return &Schema{
		Name:       name,
		DataType:   TypeRecord,
		IsNullable: nullable,
		Fields:     fields,
		Metadata:   make(map[MetadataKey]any),
	}
}

// AllFields performs a level-order traversal and returns all top-level and nested fields.
func (s *Schema) AllFields() []*Field {
	if s == nil || len(s.Fields) == 0 {
		return nil
	}
	var output []*Field
	queue := make([]*Field, 0, len(s.Fields))
	queue = append(queue, s.Fields...)

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		output = append(output, curr)

		if curr.Schema != nil && len(curr.Schema.Fields) > 0 {
			queue = append(queue, curr.Schema.Fields...)
		}
	}
	return output
}

// FieldByPath searches for a field matching the dot-delimited path.
//
// Matching prefers an exact, case-sensitive match at each level and falls back to a
// case-insensitive one only when no exact match exists. The fallback is deliberate: format adapters
// pass partition column names taken from format metadata, which does not always agree with the
// schema on case. Preferring the exact match first means a schema holding both "Name" and "name"
// resolves predictably rather than returning whichever field happened to come first.
func (s *Schema) FieldByPath(path string) *Field {
	if s == nil {
		return nil
	}
	parts := strings.Split(path, ".")
	current := s

	for i, part := range parts {
		var matched, foldMatched *Field
		for _, f := range current.Fields {
			if f.Name == part {
				matched = f
				break
			}
			if foldMatched == nil && strings.EqualFold(f.Name, part) {
				foldMatched = f
			}
		}
		if matched == nil {
			matched = foldMatched
		}
		if matched == nil {
			return nil
		}
		if i == len(parts)-1 {
			return matched
		}
		if matched.Schema == nil || matched.Schema.DataType != TypeRecord {
			return nil
		}
		current = matched.Schema
	}
	return nil
}
