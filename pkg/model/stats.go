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

// Range represents the lower and upper bounds of a column's values.
type Range struct {
	// MinValue is the minimum value in the range.
	MinValue any `json:"minValue,omitempty"`
	// MaxValue is the maximum value in the range.
	MaxValue any `json:"maxValue,omitempty"`
}

// NewRange creates a new Range with min and max values.
func NewRange(minVal, maxVal any) *Range {
	return &Range{
		MinValue: minVal,
		MaxValue: maxVal,
	}
}

// NewScalarRange creates a Range where min and max are equal (e.g. for exact partition values).
func NewScalarRange(val any) *Range {
	return &Range{
		MinValue: val,
		MaxValue: val,
	}
}

// ColumnStat represents summary statistics for a column within a data file.
type ColumnStat struct {
	// Field refers to the schema field these statistics describe.
	Field *Field `json:"field"`
	// Range is the minimum and maximum value bounds.
	Range *Range `json:"range,omitempty"`
	// NumNulls is the count of null values in this column.
	NumNulls int64 `json:"numNulls"`
	// NumNaNs is the count of NaN (Not a Number) values for floating-point columns.
	NumNaNs int64 `json:"numNaNs"`
	// TotalValues is the total count of values in this column.
	TotalValues int64 `json:"totalValues"`
}

// PartitionValue associates a partition field with its specific value range for a data file.
type PartitionValue struct {
	// PartitionField is the partition specification field.
	PartitionField *PartitionField `json:"partitionField"`
	// Range represents the partition value range (exact value for standard partitioning).
	Range *Range `json:"range"`
}
