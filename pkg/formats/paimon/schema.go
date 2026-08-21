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

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/slachiewicz/polytable/pkg/model"
)

// DataField represents a field in Apache Paimon schema.
type DataField struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// TableSchema represents an Apache Paimon schema JSON file (schema/schema-N).
type TableSchema struct {
	ID             int64             `json:"id"`
	Fields         []DataField       `json:"fields"`
	HighestFieldID int               `json:"highestFieldId"`
	PartitionKeys  []string          `json:"partitionKeys,omitempty"`
	PrimaryKeys    []string          `json:"primaryKeys,omitempty"`
	Options        map[string]string `json:"options,omitempty"`
}

// SchemaToPaimon converts canonical model.Schema into Paimon TableSchema.
func SchemaToPaimon(schema *model.Schema, partitionKeys []string) (*TableSchema, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	var fields []DataField
	highestID := 0

	for i, f := range schema.Fields {
		fID := i + 1
		if f.FieldID != nil && *f.FieldID > 0 {
			fID = *f.FieldID
		}
		if fID > highestID {
			highestID = fID
		}

		fields = append(fields, DataField{
			ID:          fID,
			Name:        f.Name,
			Type:        modelTypeToPaimonType(f.Schema),
			Description: f.Schema.Comment,
		})
	}

	return &TableSchema{
		ID:             0,
		Fields:         fields,
		HighestFieldID: highestID,
		PartitionKeys:  partitionKeys,
	}, nil
}

// PaimonToSchema converts Paimon TableSchema into canonical model.Schema.
func PaimonToSchema(ts *TableSchema) (*model.Schema, error) {
	if ts == nil {
		return nil, fmt.Errorf("paimon table schema cannot be nil")
	}

	fields := make([]*model.Field, 0, len(ts.Fields))
	for _, df := range ts.Fields {
		s, err := parsePaimonType(df.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to parse type for field %s: %w", df.Name, err)
		}
		s.Comment = df.Description

		fieldID := df.ID
		fields = append(fields, &model.Field{
			Name:    df.Name,
			FieldID: &fieldID,
			Schema:  s,
		})
	}

	return model.NewRecordSchema("paimon_table", fields, false), nil
}

func modelTypeToPaimonType(s *model.Schema) string {
	if s == nil {
		return "STRING"
	}
	base := ""
	switch s.DataType {
	case model.TypeBoolean:
		base = "BOOLEAN"
	case model.TypeInt:
		base = "INT"
	case model.TypeLong:
		base = "BIGINT"
	case model.TypeFloat:
		base = "FLOAT"
	case model.TypeDouble:
		base = "DOUBLE"
	case model.TypeString, model.TypeUUID:
		base = "STRING"
	case model.TypeBytes, model.TypeFixed:
		base = "BYTES"
	case model.TypeDate:
		base = "DATE"
	case model.TypeTimestamp, model.TypeTimestampNTZ:
		base = "TIMESTAMP(6)"
	case model.TypeDecimal:
		precision := 10
		scale := 0
		if p, ok := s.Metadata[model.MetadataKeyDecimalPrecision].(int); ok {
			precision = p
		}
		if sc, ok := s.Metadata[model.MetadataKeyDecimalScale].(int); ok {
			scale = sc
		}
		base = fmt.Sprintf("DECIMAL(%d, %d)", precision, scale)
	case model.TypeList:
		base = fmt.Sprintf("ARRAY<%s>", modelTypeToPaimonType(s.ElementSchema.Schema))
	case model.TypeMap:
		base = fmt.Sprintf("MAP<%s, %s>", modelTypeToPaimonType(s.KeySchema.Schema), modelTypeToPaimonType(s.ValueSchema.Schema))
	default:
		base = "STRING"
	}

	if !s.IsNullable {
		base += " NOT NULL"
	}
	return base
}

func parsePaimonType(raw string) (*model.Schema, error) {
	clean := strings.TrimSpace(raw)
	isNullable := true
	if strings.HasSuffix(clean, "NOT NULL") {
		isNullable = false
		clean = strings.TrimSpace(strings.TrimSuffix(clean, "NOT NULL"))
	}

	upper := strings.ToUpper(clean)
	switch {
	case upper == "BOOLEAN":
		return model.NewPrimitiveSchema(model.TypeBoolean, isNullable), nil
	case upper == "INT" || upper == "INTEGER":
		return model.NewPrimitiveSchema(model.TypeInt, isNullable), nil
	case upper == "BIGINT" || upper == "LONG":
		return model.NewPrimitiveSchema(model.TypeLong, isNullable), nil
	case upper == "FLOAT":
		return model.NewPrimitiveSchema(model.TypeFloat, isNullable), nil
	case upper == "DOUBLE":
		return model.NewPrimitiveSchema(model.TypeDouble, isNullable), nil
	case upper == "STRING" || strings.HasPrefix(upper, "VARCHAR"):
		return model.NewPrimitiveSchema(model.TypeString, isNullable), nil
	case upper == "BYTES" || upper == "BINARY":
		return model.NewPrimitiveSchema(model.TypeBytes, isNullable), nil
	case upper == "DATE":
		return model.NewPrimitiveSchema(model.TypeDate, isNullable), nil
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return model.NewPrimitiveSchema(model.TypeTimestamp, isNullable), nil
	case strings.HasPrefix(upper, "DECIMAL"):
		var p, s int
		_, _ = fmt.Sscanf(upper, "DECIMAL(%d, %d)", &p, &s)
		if p == 0 {
			p = 10
		}
		return model.NewDecimalSchema(p, s, isNullable), nil
	default:
		return model.NewPrimitiveSchema(model.TypeString, isNullable), nil
	}
}

// ParseTableSchemaJSON parses Paimon schema JSON bytes.
func ParseTableSchemaJSON(data []byte) (*TableSchema, error) {
	var ts TableSchema
	if err := json.Unmarshal(data, &ts); err != nil {
		return nil, err
	}
	return &ts, nil
}
