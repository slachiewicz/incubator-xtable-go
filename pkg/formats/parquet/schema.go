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
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/slachiewicz/polytable/pkg/model"
)

// ErrUnmappableSchema is wrapped by every error ParquetSchemaToModel returns for a column whose
// shape it cannot map onto model.Schema. Callers that want to distinguish "this Parquet file has a
// schema shape we do not support" from other failures (a read error, a malformed footer) can test
// for it with errors.Is.
var ErrUnmappableSchema = errors.New("parquet: unmappable schema")

// ParquetSchemaToModel converts a parquet-go Schema into polytable's canonical model.Schema.
//
// parquet-go's Node.Type() is documented as unsafe to call on anything but a leaf node — "Calling
// this method on non-leaf nodes will panic" — and the package backs that with real panics: the
// group, list, map and variant node types all panic out of Type().Kind() unconditionally
// (type_group.go, type_list.go, type_map.go, type_variant.go). convertField and convertType below
// are written to call Type() only once Node.Leaf() has confirmed it is safe, which is the "safe
// interrogation" T63 asked to look for; every shape that guard rejects is reported as an error
// naming the column instead.
//
// The recover here is a narrow backstop for exactly this one entry point, not a project-wide
// safety net: it wraps nothing else in the package, so a bug anywhere else in polytable panics as
// it always would. Its job is the unknown-unknowns — a parquet-go release adding a fifth
// panic-on-Type() node kind we have not audited for — where a converted panic (an error, naming
// this function) is a better failure than a crashed process, even though it cannot name the column
// the way the guarded paths do. It is a fallback for what we could not find, not a substitute for
// the guard above, which is why every path convertField and convertType know about returns a named
// error on its own rather than relying on this recover to catch it.
func ParquetSchemaToModel(schema *parquet.Schema) (result *model.Schema, err error) {
	if schema == nil {
		return nil, nil
	}

	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("%w: recovered a panic converting the parquet schema: %v", ErrUnmappableSchema, r)
		}
	}()

	fields, err := convertGroupFields(schema.Fields(), "")
	if err != nil {
		return nil, err
	}
	return model.NewRecordSchema("root", fields, false), nil
}

// convertGroupFields converts the direct children of a group node. parentPath is the dotted path
// of the group itself, empty at the root, so errors from a deeply nested column name the full path
// rather than just the leaf field's own name.
func convertGroupFields(fields []parquet.Field, parentPath string) ([]*model.Field, error) {
	modelFields := make([]*model.Field, 0, len(fields))
	for _, f := range fields {
		mf, err := convertField(f, parentPath)
		if err != nil {
			return nil, err
		}
		modelFields = append(modelFields, mf)
	}
	return modelFields, nil
}

func convertField(f parquet.Field, parentPath string) (*model.Field, error) {
	fName := f.Name()
	path := fName
	if parentPath != "" {
		path = parentPath + "." + fName
	}
	nullable := f.Optional()

	switch {
	case f.Repeated() && f.Leaf():
		// The old two-level "repeated primitive" list encoding: no wrapping LIST group, just a
		// column that repeats. Field.Type() is safe here because the column itself carries a
		// physical type.
		elemType, err := convertType(f.Type(), false, path)
		if err != nil {
			return nil, err
		}
		return &model.Field{
			Name: fName,
			Schema: &model.Schema{
				DataType:   model.TypeList,
				IsNullable: nullable,
				ElementSchema: &model.Field{
					Name:   "element",
					Schema: elemType,
				},
			},
		}, nil

	case f.Leaf():
		leafType, err := convertType(f.Type(), nullable, path)
		if err != nil {
			return nil, err
		}
		return &model.Field{Name: fName, Schema: leafType}, nil

	case f.Repeated():
		// Non-leaf and repeated: the modern three-level LIST encoding's "list" node, or a MAP's
		// "key_value" node, both of which repeat and carry child fields (element, or key/value)
		// rather than a physical type. parquet-go's Field.Type() panics on exactly this shape
		// (type_list.go, type_map.go, type_group.go all panic in Kind()), so it is never called
		// here; the shape is reported instead of guessed at, since collapsing it into a plain
		// struct would silently drop the list/map cardinality it represents.
		return nil, fmt.Errorf("column %q: %w: repeated group with %d child field(s) "+
			"(LIST/MAP-shaped nested repetition is not supported)", path, ErrUnmappableSchema, len(f.Fields()))

	default:
		// Non-leaf, non-repeated: an ordinary nested struct/record. This also covers a group with
		// zero children, which is a legal (if unusual) empty struct rather than an error.
		childFields, err := convertGroupFields(f.Fields(), path)
		if err != nil {
			return nil, err
		}
		recordSchema := model.NewRecordSchema(fName, childFields, nullable)
		return &model.Field{Name: fName, Schema: recordSchema}, nil
	}
}

// convertType maps a leaf parquet-go Type onto the canonical model. Every caller has already
// confirmed, via Field.Leaf(), that t was obtained safely; convertType does not itself guard
// against a group/list/map/variant type; passing one of those would panic in t.Kind(), exactly the
// shape convertField's Leaf() check exists to keep out.
func convertType(t parquet.Type, nullable bool, path string) (*model.Schema, error) {
	if t == nil {
		return nil, fmt.Errorf("column %q: %w: no physical or logical type", path, ErrUnmappableSchema)
	}

	// Check logical types first. parquet-go v0.32 models the thrift union as a sum type, so this
	// switches on the single Value field rather than testing one pointer field per member.
	if lt := t.LogicalType(); lt != nil {
		switch v := lt.Value.(type) {
		case *format.StringType, *format.EnumType:
			return model.NewPrimitiveSchema(model.TypeString, nullable), nil
		case *format.DateType:
			return model.NewPrimitiveSchema(model.TypeDate, nullable), nil
		case *format.TimestampType:
			return model.NewPrimitiveSchema(model.TypeTimestamp, nullable), nil
		case *format.UUIDType:
			return model.NewPrimitiveSchema(model.TypeUUID, nullable), nil
		case *format.DecimalType:
			return model.NewDecimalSchema(int(v.Precision), int(v.Scale), nullable), nil
		}
	}

	// Fall back to physical parquet types.
	switch t.Kind() {
	case parquet.Boolean:
		return model.NewPrimitiveSchema(model.TypeBoolean, nullable), nil
	case parquet.Int32:
		return model.NewPrimitiveSchema(model.TypeInt, nullable), nil
	case parquet.Int64:
		return model.NewPrimitiveSchema(model.TypeLong, nullable), nil
	case parquet.Float:
		return model.NewPrimitiveSchema(model.TypeFloat, nullable), nil
	case parquet.Double:
		return model.NewPrimitiveSchema(model.TypeDouble, nullable), nil
	case parquet.ByteArray:
		return model.NewPrimitiveSchema(model.TypeString, nullable), nil
	case parquet.FixedLenByteArray:
		return model.NewPrimitiveSchema(model.TypeBytes, nullable), nil
	default:
		return model.NewPrimitiveSchema(model.TypeString, nullable), nil
	}
}
