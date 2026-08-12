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
	"path/filepath"
	"strings"

	"github.com/apache/incubator-xtable-go/pkg/model"
)

// ExtractHivePartitions parses directory segments between basePath and filePath for Hive-style key=value pairs.
func ExtractHivePartitions(filePath, basePath string, schema *model.Schema) ([]*model.PartitionField, []*model.PartitionValue) {
	relPath := strings.TrimPrefix(filePath, basePath)
	relPath = strings.TrimPrefix(relPath, "/")
	dir := filepath.Dir(relPath)

	if dir == "." || dir == "" {
		return nil, nil
	}

	segments := strings.Split(dir, "/")
	var partFields []*model.PartitionField
	var partValues []*model.PartitionValue

	for _, seg := range segments {
		parts := strings.SplitN(seg, "=", 2)
		if len(parts) == 2 {
			colName := parts[0]
			colVal := parts[1]

			var field *model.Field
			if schema != nil {
				field = schema.FieldByPath(colName)
			}
			if field == nil {
				field = &model.Field{
					Name:   colName,
					Schema: model.NewPrimitiveSchema(model.TypeString, true),
				}
			}

			pf := &model.PartitionField{
				SourceField:   field,
				TransformType: model.PartitionTransformValue,
			}
			partFields = append(partFields, pf)

			pv := &model.PartitionValue{
				PartitionField: pf,
				Range:          model.NewScalarRange(colVal),
			}
			partValues = append(partValues, pv)
		}
	}

	return partFields, partValues
}
