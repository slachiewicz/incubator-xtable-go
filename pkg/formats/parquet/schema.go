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

package parquet

import (
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/slachiewicz/xtable-go/pkg/model"
)

// ParquetSchemaToModel converts a parquet-go Schema into XTable's canonical model.Schema.
func ParquetSchemaToModel(schema *parquet.Schema) *model.Schema {
	if schema == nil {
		return nil
	}
	fields := convertGroupFields(schema.Fields())
	return model.NewRecordSchema("root", fields, false)
}

func convertGroupFields(fields []parquet.Field) []*model.Field {
	var modelFields []*model.Field
	for _, f := range fields {
		modelFields = append(modelFields, convertField(f))
	}
	return modelFields
}

func convertField(f parquet.Field) *model.Field {
	fName := f.Name()
	nullable := f.Optional()

	if f.Repeated() {
		// List type
		elemType := convertType(f.Type(), false)
		listSchema := &model.Schema{
			DataType:   model.TypeList,
			IsNullable: nullable,
			ElementSchema: &model.Field{
				Name:   "element",
				Schema: elemType,
			},
		}
		return &model.Field{
			Name:   fName,
			Schema: listSchema,
		}
	}

	if len(f.Fields()) > 0 {
		// Nested Group / Struct
		childFields := convertGroupFields(f.Fields())
		recordSchema := model.NewRecordSchema(fName, childFields, nullable)
		return &model.Field{
			Name:   fName,
			Schema: recordSchema,
		}
	}

	// Leaf primitive field
	leafSchema := convertType(f.Type(), nullable)
	return &model.Field{
		Name:   fName,
		Schema: leafSchema,
	}
}

func convertType(t parquet.Type, nullable bool) *model.Schema {
	if t == nil {
		return model.NewPrimitiveSchema(model.TypeString, nullable)
	}

	// Check logical types first. parquet-go v0.32 models the thrift union as a sum type, so this
	// switches on the single Value field rather than testing one pointer field per member.
	if lt := t.LogicalType(); lt != nil {
		switch v := lt.Value.(type) {
		case *format.StringType, *format.EnumType:
			return model.NewPrimitiveSchema(model.TypeString, nullable)
		case *format.DateType:
			return model.NewPrimitiveSchema(model.TypeDate, nullable)
		case *format.TimestampType:
			return model.NewPrimitiveSchema(model.TypeTimestamp, nullable)
		case *format.UUIDType:
			return model.NewPrimitiveSchema(model.TypeUUID, nullable)
		case *format.DecimalType:
			return model.NewDecimalSchema(int(v.Precision), int(v.Scale), nullable)
		}
	}

	// Fall back to physical parquet types
	switch t.Kind() {
	case parquet.Boolean:
		return model.NewPrimitiveSchema(model.TypeBoolean, nullable)
	case parquet.Int32:
		return model.NewPrimitiveSchema(model.TypeInt, nullable)
	case parquet.Int64:
		return model.NewPrimitiveSchema(model.TypeLong, nullable)
	case parquet.Float:
		return model.NewPrimitiveSchema(model.TypeFloat, nullable)
	case parquet.Double:
		return model.NewPrimitiveSchema(model.TypeDouble, nullable)
	case parquet.ByteArray:
		return model.NewPrimitiveSchema(model.TypeString, nullable)
	case parquet.FixedLenByteArray:
		return model.NewPrimitiveSchema(model.TypeBytes, nullable)
	default:
		return model.NewPrimitiveSchema(model.TypeString, nullable)
	}
}
