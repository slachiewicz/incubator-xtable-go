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

package delta

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/slachiewicz/xtable-go/pkg/model"
)

// DeltaStructField matches a field in Delta schemaString JSON.
type DeltaStructField struct {
	Name     string          `json:"name"`
	Type     json.RawMessage `json:"type"`
	Nullable bool            `json:"nullable"`
	Metadata map[string]any  `json:"metadata,omitempty"`
}

// DeltaStructType matches a struct in Delta schemaString JSON.
type DeltaStructType struct {
	Type   string              `json:"type"`
	Fields []*DeltaStructField `json:"fields"`
}

// DeltaArrayType matches an array in Delta schemaString JSON.
type DeltaArrayType struct {
	Type         string          `json:"type"`
	ElementType  json.RawMessage `json:"elementType"`
	ContainsNull bool            `json:"containsNull"`
}

// DeltaMapType matches a map in Delta schemaString JSON.
type DeltaMapType struct {
	Type              string          `json:"type"`
	KeyType           json.RawMessage `json:"keyType"`
	ValueType         json.RawMessage `json:"valueType"`
	ValueContainsNull bool            `json:"valueContainsNull"`
}

// SchemaToDeltaJSON converts a canonical model.Schema to Delta schemaString JSON.
func SchemaToDeltaJSON(schema *model.Schema) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema cannot be nil")
	}
	deltaStruct, err := toDeltaStruct(schema)
	if err != nil {
		return "", err
	}
	bytes, err := json.Marshal(deltaStruct)
	if err != nil {
		return "", fmt.Errorf("failed to marshal delta schema: %w", err)
	}
	return string(bytes), nil
}

func toDeltaStruct(s *model.Schema) (*DeltaStructType, error) {
	fields := make([]*DeltaStructField, 0, len(s.Fields))
	for _, f := range s.Fields {
		typeJSON, err := typeToDeltaJSON(f.Schema)
		if err != nil {
			return nil, err
		}
		fields = append(fields, &DeltaStructField{
			Name:     f.Name,
			Type:     typeJSON,
			Nullable: f.Schema.IsNullable,
			Metadata: make(map[string]any),
		})
	}
	return &DeltaStructType{
		Type:   "struct",
		Fields: fields,
	}, nil
}

func typeToDeltaJSON(s *model.Schema) (json.RawMessage, error) {
	if s == nil {
		return json.Marshal("string")
	}

	switch s.DataType {
	case model.TypeBoolean:
		return json.Marshal("boolean")
	case model.TypeInt:
		return json.Marshal("integer")
	case model.TypeLong:
		return json.Marshal("long")
	case model.TypeFloat:
		return json.Marshal("float")
	case model.TypeDouble:
		return json.Marshal("double")
	case model.TypeString, model.TypeEnum, model.TypeUUID:
		return json.Marshal("string")
	case model.TypeBytes, model.TypeFixed:
		return json.Marshal("binary")
	case model.TypeDate:
		return json.Marshal("date")
	case model.TypeTimestamp:
		return json.Marshal("timestamp")
	case model.TypeTimestampNTZ:
		return json.Marshal("timestamp_ntz")
	case model.TypeDecimal:
		precision := 10
		scale := 0
		if p, ok := s.Metadata[model.MetadataKeyDecimalPrecision].(int); ok {
			precision = p
		}
		if sc, ok := s.Metadata[model.MetadataKeyDecimalScale].(int); ok {
			scale = sc
		}
		return json.Marshal(fmt.Sprintf("decimal(%d,%d)", precision, scale))
	case model.TypeRecord:
		st, err := toDeltaStruct(s)
		if err != nil {
			return nil, err
		}
		bytes, err := json.Marshal(st)
		return json.RawMessage(bytes), err
	case model.TypeList:
		elemType, err := typeToDeltaJSON(s.ElementSchema.Schema)
		if err != nil {
			return nil, err
		}
		arr := DeltaArrayType{
			Type:         "array",
			ElementType:  elemType,
			ContainsNull: s.ElementSchema.Schema.IsNullable,
		}
		bytes, err := json.Marshal(arr)
		return json.RawMessage(bytes), err
	case model.TypeMap:
		kType, err := typeToDeltaJSON(s.KeySchema.Schema)
		if err != nil {
			return nil, err
		}
		vType, err := typeToDeltaJSON(s.ValueSchema.Schema)
		if err != nil {
			return nil, err
		}
		m := DeltaMapType{
			Type:              "map",
			KeyType:           kType,
			ValueType:         vType,
			ValueContainsNull: s.ValueSchema.Schema.IsNullable,
		}
		bytes, err := json.Marshal(m)
		return json.RawMessage(bytes), err
	default:
		return json.Marshal("string")
	}
}

// DeltaJSONToSchema parses a Delta schemaString JSON into a canonical model.Schema.
func DeltaJSONToSchema(schemaString string) (*model.Schema, error) {
	var structType DeltaStructType
	if err := json.Unmarshal([]byte(schemaString), &structType); err != nil {
		return nil, fmt.Errorf("invalid delta schema JSON: %w", err)
	}

	fields := make([]*model.Field, 0, len(structType.Fields))
	for _, df := range structType.Fields {
		fSchema, err := parseDeltaType(df.Type, df.Nullable)
		if err != nil {
			return nil, fmt.Errorf("failed to parse field %s: %w", df.Name, err)
		}
		fields = append(fields, &model.Field{
			Name:   df.Name,
			Schema: fSchema,
		})
	}

	return model.NewRecordSchema("root", fields, false), nil
}

func parseDeltaType(raw json.RawMessage, nullable bool) (*model.Schema, error) {
	// First check if it's a simple string type
	var typeStr string
	if err := json.Unmarshal(raw, &typeStr); err == nil {
		typeStr = strings.ToLower(typeStr)
		switch {
		case typeStr == "boolean":
			return model.NewPrimitiveSchema(model.TypeBoolean, nullable), nil
		case typeStr == "integer" || typeStr == "int" || typeStr == "short" || typeStr == "byte":
			return model.NewPrimitiveSchema(model.TypeInt, nullable), nil
		case typeStr == "long":
			return model.NewPrimitiveSchema(model.TypeLong, nullable), nil
		case typeStr == "float":
			return model.NewPrimitiveSchema(model.TypeFloat, nullable), nil
		case typeStr == "double":
			return model.NewPrimitiveSchema(model.TypeDouble, nullable), nil
		case typeStr == "string":
			return model.NewPrimitiveSchema(model.TypeString, nullable), nil
		case typeStr == "binary":
			return model.NewPrimitiveSchema(model.TypeBytes, nullable), nil
		case typeStr == "date":
			return model.NewPrimitiveSchema(model.TypeDate, nullable), nil
		case typeStr == "timestamp":
			return model.NewPrimitiveSchema(model.TypeTimestamp, nullable), nil
		case typeStr == "timestamp_ntz":
			return model.NewPrimitiveSchema(model.TypeTimestampNTZ, nullable), nil
		case strings.HasPrefix(typeStr, "decimal"):
			// Parse decimal(precision, scale)
			p, s := 10, 0
			trimmed := strings.TrimPrefix(typeStr, "decimal(")
			trimmed = strings.TrimSuffix(trimmed, ")")
			parts := strings.Split(trimmed, ",")
			if len(parts) == 2 {
				if parsedP, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
					p = parsedP
				}
				if parsedS, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					s = parsedS
				}
			}
			return model.NewDecimalSchema(p, s, nullable), nil
		default:
			return model.NewPrimitiveSchema(model.TypeString, nullable), nil
		}
	}

	// Try nested Struct
	var structType DeltaStructType
	if err := json.Unmarshal(raw, &structType); err == nil && structType.Type == "struct" {
		fields := make([]*model.Field, 0, len(structType.Fields))
		for _, f := range structType.Fields {
			childSchema, err := parseDeltaType(f.Type, f.Nullable)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &model.Field{
				Name:   f.Name,
				Schema: childSchema,
			})
		}
		return model.NewRecordSchema("", fields, nullable), nil
	}

	// Try Array
	var arrType DeltaArrayType
	if err := json.Unmarshal(raw, &arrType); err == nil && arrType.Type == "array" {
		elemSchema, err := parseDeltaType(arrType.ElementType, arrType.ContainsNull)
		if err != nil {
			return nil, err
		}
		return &model.Schema{
			DataType:   model.TypeList,
			IsNullable: nullable,
			ElementSchema: &model.Field{
				Name:   "element",
				Schema: elemSchema,
			},
		}, nil
	}

	// Try Map
	var mapType DeltaMapType
	if err := json.Unmarshal(raw, &mapType); err == nil && mapType.Type == "map" {
		kSchema, err := parseDeltaType(mapType.KeyType, false)
		if err != nil {
			return nil, err
		}
		vSchema, err := parseDeltaType(mapType.ValueType, mapType.ValueContainsNull)
		if err != nil {
			return nil, err
		}
		return &model.Schema{
			DataType:   model.TypeMap,
			IsNullable: nullable,
			KeySchema: &model.Field{
				Name:   "key",
				Schema: kSchema,
			},
			ValueSchema: &model.Field{
				Name:   "value",
				Schema: vSchema,
			},
		}, nil
	}

	return nil, fmt.Errorf("unrecognized delta type structure: %s", string(raw))
}
