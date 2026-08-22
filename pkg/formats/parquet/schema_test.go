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

package parquet_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pqformat "github.com/slachiewicz/polytable/pkg/formats/parquet"
	"github.com/slachiewicz/polytable/pkg/model"
)

// deltaCheckpointFixture is a real delta-rs-written checkpoint, kept for T45's negative control.
// Its nested schema shape (a MAP<string,string> column, "add.partitionValues", encoded as a
// repeated "key_value" group with "key" and "value" children) is what T63 found: parquet-go's
// Field.Type() panics when called on a repeated node that is not a leaf, and the pre-fix converter
// called it unconditionally on every repeated field.
const deltaCheckpointFixture = "../../../test/testdata/fixtures/delta-rs-checkpoint/orders/_delta_log/00000000000000000002.checkpoint.parquet"

// openParquetFile is a small helper shared by the tests below: it reads a file from disk and hands
// it to parquet.OpenFile the same way pkg/formats/parquet/source.go does.
func openParquetFile(t *testing.T, path string) *parquet.File {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "fixture %s must exist", path)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err, "fixture %s must be a well-formed parquet file", path)
	return f
}

// TestParquetSchemaToModel_NestedGroupShapes is T63's acceptance test: every schema shape whose
// Field.Type() parquet-go documents (and enforces, by panicking) as unsafe to call outside a leaf
// node. parquet-go's own source names four culprits — type_group.go, type_list.go, type_map.go and
// type_variant.go all panic unconditionally in Kind() — and this table drives ParquetSchemaToModel
// through the three of them reachable from ordinary Go types (group, list, map; a VARIANT column
// has no parquet-go struct tag and is out of scope here) plus the real fixture that first found
// this, plus one positive case pinning the one repeated shape that is safe.
//
// A hand-written file of garbage bytes cannot reproduce any of these: it fails cleanly in
// parquet.OpenFile and never reaches the schema converter at all, which is why T45's separate
// metadata exclusion left this defect standing until a real writer's footer exercised it. The
// list/map cases below are built directly with parquet.List/parquet.Map, mirroring how
// TestParquetSchemaToModel_LogicalTypes already builds schemas in this package, and are confirmed
// (see the package's development notes) to reproduce the identical shape a real
// parquet.GenericWriter-written, then parquet.OpenFile-reopened, LIST/MAP column has: a
// non-repeated non-leaf wrapper ("items", "attrs") containing a repeated non-leaf group ("list",
// "key_value") that is where the panic actually occurred.
func TestParquetSchemaToModel_NestedGroupShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		schema      func(t *testing.T) *parquet.Schema
		wantErr     bool
		wantPathHas string // substring the full dotted column path in the error must contain
	}{
		{
			name: "delta-rs checkpoint: MAP<string,string> as a repeated key_value group",
			schema: func(t *testing.T) *parquet.Schema {
				t.Helper()
				return openParquetFile(t, deltaCheckpointFixture).Schema()
			},
			wantErr:     true,
			wantPathHas: "add.partitionValues.key_value",
		},
		{
			name: "hand-built MAP<string,string>: repeated key_value group",
			schema: func(*testing.T) *parquet.Schema {
				return parquet.NewSchema("root", parquet.Group{
					"attrs": parquet.Map(parquet.String(), parquet.String()),
				})
			},
			wantErr:     true,
			wantPathHas: "attrs.key_value",
		},
		{
			name: "hand-built LIST<string> (3-level encoding): repeated list group",
			schema: func(*testing.T) *parquet.Schema {
				return parquet.NewSchema("root", parquet.Group{
					"items": parquet.List(parquet.String()),
				})
			},
			wantErr:     true,
			wantPathHas: "items.list",
		},
		{
			name: "repeated leaf column (2-level encoding, no wrapper group) still converts",
			schema: func(*testing.T) *parquet.Schema {
				return parquet.NewSchema("root", parquet.Group{
					"tags": parquet.Repeated(parquet.String()),
				})
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schema := tt.schema(t)
			got, err := pqformat.ParquetSchemaToModel(schema)

			// The defect this guards against is a panic, not merely a wrong answer, so the
			// assertion that matters most is that this line is reached at all: if
			// ParquetSchemaToModel panics, the test process reports this test as failed (or
			// crashes outright) rather than reaching these assertions. The package's development
			// notes record running this table by hand against the pre-fix converter and observing
			// exactly that panic, with the identical stack trace this defect was found with.
			if !tt.wantErr {
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Len(t, got.Fields, 1)
				assert.Equal(t, model.TypeList, got.Fields[0].Schema.DataType)
				return
			}

			require.Error(t, err, "this schema shape must be reported as unmappable, not silently accepted")
			assert.True(t, errors.Is(err, pqformat.ErrUnmappableSchema),
				"the error should wrap ErrUnmappableSchema so callers can distinguish this from other failures: %v", err)
			assert.Nil(t, got)

			// The message must name the offending column's full path, not just its own field name
			// or say "something failed" — T63 calls out a wide table with one bad column as the
			// common case, and the full path is also what distinguishes this from the recover
			// backstop: a panic caught there names no column at all.
			assert.Contains(t, err.Error(), tt.wantPathHas,
				"the error should name the full path of the repeated-group column it could not map")
		})
	}
}

// TestParquetSchemaToModel_ExistingFixturesUnchanged asserts the fix narrowed nothing: an ordinary
// flat schema, with no MAP/LIST-shaped repetition, still converts without error.
func TestParquetSchemaToModel_ExistingFixturesUnchanged(t *testing.T) {
	t.Parallel()

	records := []TestRecord{
		{ID: 1, Name: "Alice", Score: 95.5},
		{ID: 2, Name: "Bob", Score: 88.0},
	}
	data := createParquetBytes(t, records)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	schema, err := pqformat.ParquetSchemaToModel(f.Schema())
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Len(t, schema.Fields, 3)

	names := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		names = append(names, field.Name)
	}
	assert.ElementsMatch(t, []string{"id", "name", "score"}, names)
}
