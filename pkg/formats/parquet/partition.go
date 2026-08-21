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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/slachiewicz/polytable/pkg/model"
)

// hiveNullMarker is the directory name Hive, Spark and Trino write for a null partition value.
const hiveNullMarker = "__HIVE_DEFAULT_PARTITION__"

// HivePartition is one key=value directory segment of a Hive-style layout.
type HivePartition struct {
	Key   string
	Value string
}

// HivePartitionsForFile parses the directory segments between basePath and filePath for Hive-style
// key=value pairs, outermost first. Segments without an "=" are not partitions and are skipped.
func HivePartitionsForFile(filePath, basePath string) []HivePartition {
	relPath := strings.TrimPrefix(filePath, basePath)
	relPath = strings.TrimPrefix(relPath, "/")
	dir := filepath.ToSlash(filepath.Dir(relPath))

	if dir == "." || dir == "" {
		return nil
	}

	var partitions []HivePartition
	for _, segment := range strings.Split(dir, "/") {
		key, value, found := strings.Cut(segment, "=")
		if !found {
			continue
		}
		partitions = append(partitions, HivePartition{Key: key, Value: value})
	}
	return partitions
}

// PartitionColumnSchema infers the type of a Hive partition column from the values seen in the
// directory names, since no data file carries the column to read a type from: LONG when every value
// is an integer, DOUBLE when every value is numeric, DATE when every value is an ISO date, and
// STRING otherwise.
//
// Anything ambiguous is a STRING — no values at all, an empty value, or Hive's null marker — which
// is the one type the raw directory names always fit. The column is nullable: no data file
// constrains it, and a partition whose value is the null marker has no value at all.
func PartitionColumnSchema(values []string) *model.Schema {
	if len(values) == 0 {
		return model.NewPrimitiveSchema(model.TypeString, true)
	}

	integral, numeric, dated := true, true, true
	for _, value := range values {
		if value == "" || value == hiveNullMarker {
			return model.NewPrimitiveSchema(model.TypeString, true)
		}
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			integral = false
		}
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			numeric = false
		}
		if _, err := time.Parse(time.DateOnly, value); err != nil {
			dated = false
		}
	}

	switch {
	case integral:
		return model.NewPrimitiveSchema(model.TypeLong, true)
	case numeric:
		return model.NewPrimitiveSchema(model.TypeDouble, true)
	case dated:
		return model.NewPrimitiveSchema(model.TypeDate, true)
	default:
		return model.NewPrimitiveSchema(model.TypeString, true)
	}
}

// partitionSample is one observed directory value and a file it was observed on.
type partitionSample struct {
	value string
	file  string
}

// observedPartition collects everything the directory names said about one partition column.
type observedPartition struct {
	key     string
	samples []partitionSample
}

func (o *observedPartition) values() []string {
	values := make([]string, 0, len(o.samples))
	for _, sample := range o.samples {
		values = append(values, sample.value)
	}
	return values
}

// partitionSpec resolves each partition column against the read schema and returns the schema the
// table should report together with its partition spec, in directory order.
//
// A Hive partition column lives in the directory name and nowhere else, so a schema built from the
// data files alone does not define the column the same table is partitioned by. The column is
// synthesized here and appended after the physical ones, with a type inferred from the values.
//
// A data file carrying a column of that name wins: the file is the authority on the type, and the
// directory values have to be readable as it, or the table would describe itself wrongly. That
// check is why this can fail.
func partitionSpec(schema *model.Schema, observed []observedPartition) (*model.Schema, []*model.PartitionField, error) {
	if len(observed) == 0 {
		return schema, nil, nil
	}

	fields := make([]*model.PartitionField, 0, len(observed))
	for i := range observed {
		column := &observed[i]

		// FieldByPath, not a name comparison: a "Region=eu" directory over a "region" column is the
		// same column under a different spelling, not a second one.
		sourceField := schema.FieldByPath(column.key)
		if sourceField == nil {
			sourceField = &model.Field{
				Name:   column.key,
				Schema: PartitionColumnSchema(column.values()),
			}
			schema.Fields = append(schema.Fields, sourceField)
		} else if err := checkValuesFitColumn(column, sourceField); err != nil {
			return nil, nil, err
		}

		fields = append(fields, &model.PartitionField{
			SourceField:   sourceField,
			TransformType: model.PartitionTransformValue,
		})
	}
	return schema, fields, nil
}

// checkValuesFitColumn reports a directory value that cannot be read as the type the data files
// give the column it collides with.
func checkValuesFitColumn(column *observedPartition, field *model.Field) error {
	for _, sample := range column.samples {
		if valueFitsType(sample.value, field.Schema) {
			continue
		}
		return fmt.Errorf(
			"partition directory %s=%s does not fit column %q, which the data files declare %s (%s)",
			column.key, sample.value, field.Path(), typeSignature(field.Schema), sample.file)
	}
	return nil
}

// valueFitsType reports whether a raw directory value can be read as the given type. Types a
// directory name has no single spelling for — timestamps above all — are accepted rather than
// guessed at. The null marker fits everything: it stands for no value.
func valueFitsType(value string, schema *model.Schema) bool {
	if schema == nil || value == "" || value == hiveNullMarker {
		return true
	}
	switch schema.DataType {
	case model.TypeInt, model.TypeLong:
		_, err := strconv.ParseInt(value, 10, 64)
		return err == nil
	case model.TypeFloat, model.TypeDouble, model.TypeDecimal:
		_, err := strconv.ParseFloat(value, 64)
		return err == nil
	case model.TypeBoolean:
		_, err := strconv.ParseBool(value)
		return err == nil
	case model.TypeDate:
		_, err := time.Parse(time.DateOnly, value)
		return err == nil
	default:
		return true
	}
}
