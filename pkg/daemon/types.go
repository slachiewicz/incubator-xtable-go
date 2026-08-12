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

package daemon

import (
	"time"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/model"
	"github.com/slachiewicz/xtable-go/pkg/spi"
)

// ConvertTableRequest represents the JSON request payload for POST /v1/conversion/table.
type ConvertTableRequest struct {
	SourceFormat   model.TableFormat         `json:"sourceFormat"`
	TargetFormats  []model.TableFormat       `json:"targetFormats"`
	TableName      string                    `json:"tableName,omitempty"`
	TableBasePath  string                    `json:"tableBasePath"`
	TableDataPath  string                    `json:"tableDataPath,omitempty"`
	Namespace      string                    `json:"namespace,omitempty"`
	SyncMode       spi.SyncMode              `json:"syncMode,omitempty"`
	Configurations map[string]string         `json:"configurations,omitempty"`
	Storage        *conversion.StorageConfig `json:"storage,omitempty"`
}

// ConvertTableResponse represents the JSON response for conversion operations.
type ConvertTableResponse struct {
	ConversionID string                                `json:"conversionId"`
	Status       string                                `json:"status"` // COMPLETED, RUNNING, FAILED
	SubmittedAt  time.Time                             `json:"submittedAt,omitempty"`
	FinishedAt   *time.Time                            `json:"finishedAt,omitempty"`
	Results      map[model.TableFormat]*spi.SyncResult `json:"results,omitempty"`
	Error        string                                `json:"error,omitempty"`
}

// InspectTableRequest represents POST /v1/conversion/inspect.
type InspectTableRequest struct {
	Format        model.TableFormat         `json:"format"`
	TableBasePath string                    `json:"tableBasePath"`
	Storage       *conversion.StorageConfig `json:"storage,omitempty"`
}

// InspectTableResponse represents the response containing table metadata.
type InspectTableResponse struct {
	TableName            string            `json:"tableName"`
	Format               model.TableFormat `json:"format"`
	TableBasePath        string            `json:"tableBasePath"`
	LatestCommitTime     int64             `json:"latestCommitTime"`
	ActiveDataFilesCount int               `json:"activeDataFilesCount"`
	PartitionFields      []string          `json:"partitionFields,omitempty"`
	Schema               *model.Schema     `json:"schema"`
}

// HealthStatus represents GET /v1/health response.
type HealthStatus struct {
	Status    string    `json:"status"`
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}
