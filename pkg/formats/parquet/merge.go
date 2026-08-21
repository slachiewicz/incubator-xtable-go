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
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/slachiewicz/polytable/pkg/model"
)

// FooterSchema is the schema of one data file, paired with the file it was read from.
type FooterSchema struct {
	// Path is the file the footer came from. It names the file in a conflict error.
	Path string
	// ModTime is the file's modification time. The newest footer decides column order.
	ModTime time.Time
	// Schema is the footer converted onto the canonical model.
	Schema *model.Schema
}

// MergeFooterSchemas folds the footers of a Parquet directory into one read schema.
//
// An unmanaged directory has no log to read a schema from, so the schema is whatever the data files
// say. Taking one file's footer makes the answer depend on which file that is, which on a
// schema-evolved table means the reported columns depend on file names. The merge instead is the
// union of every footer: the newest file's columns come first, in its order, and a column only
// older files carry is appended after them.
//
// A column absent from some files is nullable in the merge, because the rows of those files have no
// value for it. A column two files give different types is an error naming the column, both types
// and both files — the alternative is picking one silently and reporting a schema half the data
// does not match. Types must be identical, not merely compatible: an INT column and a LONG column
// of the same name conflict rather than widening, since a widened schema is still a guess about
// what the writer meant.
//
// Files sharing a modification time are ordered by path, which keeps the result deterministic. Only
// the column order can move in that case; the set of columns and their types cannot.
func MergeFooterSchemas(footers []FooterSchema) (*model.Schema, error) {
	ordered := make([]FooterSchema, 0, len(footers))
	for _, footer := range footers {
		if footer.Schema != nil {
			ordered = append(ordered, footer)
		}
	}
	if len(ordered) == 0 {
		return nil, nil
	}

	slices.SortStableFunc(ordered, func(a, b FooterSchema) int {
		if c := b.ModTime.Compare(a.ModTime); c != 0 {
			return c
		}
		return strings.Compare(a.Path, b.Path)
	})

	parts := make([]recordPart, 0, len(ordered))
	for _, footer := range ordered {
		parts = append(parts, recordPart{file: footer.Path, schema: footer.Schema})
	}
	return mergeRecords(parts, "")
}

// recordPart is one file's view of a record: the top-level schema for the file itself, or a nested
// struct column of it.
type recordPart struct {
	file   string
	schema *model.Schema
}

// fieldPart is one file's view of a single column.
type fieldPart struct {
	file  string
	field *model.Field
}

// mergeRecords merges the record schemas that several files hold at the same path.
func mergeRecords(parts []recordPart, parent string) (*model.Schema, error) {
	var order []string
	occurrences := make(map[string][]fieldPart)
	for _, part := range parts {
		for _, field := range part.schema.Fields {
			if _, seen := occurrences[field.Name]; !seen {
				order = append(order, field.Name)
			}
			occurrences[field.Name] = append(occurrences[field.Name], fieldPart{file: part.file, field: field})
		}
	}

	merged := make([]*model.Field, 0, len(order))
	for _, name := range order {
		found := occurrences[name]
		path := name
		if parent != "" {
			path = parent + "." + name
		}

		// Absent from a file means null for that file's rows, so a column not every file carries is
		// nullable whatever the files that do carry it declare.
		nullable := len(found) < len(parts)
		for _, occurrence := range found {
			if occurrence.field.Schema == nil {
				return nil, fmt.Errorf("column %q of %s has no type", path, occurrence.file)
			}
			nullable = nullable || occurrence.field.Schema.IsNullable
		}

		first := found[0]
		for _, occurrence := range found[1:] {
			if err := checkTypesAgree(path, first, occurrence); err != nil {
				return nil, err
			}
		}

		var schema *model.Schema
		if first.field.Schema.DataType == model.TypeRecord {
			nested := make([]recordPart, 0, len(found))
			for _, occurrence := range found {
				nested = append(nested, recordPart{file: occurrence.file, schema: occurrence.field.Schema})
			}
			nestedSchema, err := mergeRecords(nested, path)
			if err != nil {
				return nil, err
			}
			schema = nestedSchema
		} else {
			schema = cloneSchema(first.field.Schema)
		}
		schema.IsNullable = nullable

		merged = append(merged, &model.Field{
			Name:         name,
			ParentPath:   first.field.ParentPath,
			Schema:       schema,
			FieldID:      first.field.FieldID,
			DefaultValue: first.field.DefaultValue,
		})
	}

	return model.NewRecordSchema(parts[0].schema.Name, merged, parts[0].schema.IsNullable), nil
}

// checkTypesAgree reports a conflict when two files describe the same column differently. Struct
// columns are compared by kind only: their children are merged recursively, so a struct that gained
// a field is an evolution rather than a conflict.
func checkTypesAgree(path string, first, other fieldPart) error {
	if first.field.Schema.DataType == model.TypeRecord && other.field.Schema.DataType == model.TypeRecord {
		return nil
	}
	firstType, otherType := typeSignature(first.field.Schema), typeSignature(other.field.Schema)
	if firstType == otherType {
		return nil
	}
	return fmt.Errorf("column %q is %s in %s and %s in %s", path, firstType, first.file, otherType, other.file)
}

// typeSignature renders a type the way a conflict message should read it: precise enough that two
// signatures being equal means the two types are interchangeable.
func typeSignature(schema *model.Schema) string {
	if schema == nil {
		return "<none>"
	}
	switch schema.DataType {
	case model.TypeDecimal:
		return fmt.Sprintf("DECIMAL(%v,%v)",
			schema.Metadata[model.MetadataKeyDecimalPrecision], schema.Metadata[model.MetadataKeyDecimalScale])
	case model.TypeList:
		return "LIST<" + fieldSignature(schema.ElementSchema) + ">"
	case model.TypeMap:
		return "MAP<" + fieldSignature(schema.KeySchema) + "," + fieldSignature(schema.ValueSchema) + ">"
	case model.TypeRecord:
		names := make([]string, 0, len(schema.Fields))
		for _, field := range schema.Fields {
			names = append(names, field.Name+":"+typeSignature(field.Schema))
		}
		return "RECORD<" + strings.Join(names, ",") + ">"
	default:
		return string(schema.DataType)
	}
}

func fieldSignature(field *model.Field) string {
	if field == nil {
		return "<none>"
	}
	return typeSignature(field.Schema)
}

// cloneSchema copies a leaf schema so that the merged schema does not alias the per-file ones.
func cloneSchema(schema *model.Schema) *model.Schema {
	clone := *schema
	if schema.Metadata != nil {
		clone.Metadata = make(map[model.MetadataKey]any, len(schema.Metadata))
		for key, value := range schema.Metadata {
			clone.Metadata[key] = value
		}
	}
	return &clone
}
