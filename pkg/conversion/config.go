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

package conversion

import (
	"fmt"

	"github.com/apache/incubator-xtable-go/pkg/model"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

// DatasetConfig defines the synchronization configuration for a single table dataset.
type DatasetConfig struct {
	// SourceFormat is the input table format (e.g. DELTA, ICEBERG, HUDI).
	SourceFormat model.TableFormat `json:"sourceFormat" yaml:"sourceFormat"`
	// TargetFormats is the list of formats to synchronize to (e.g. [ICEBERG, DELTA]).
	TargetFormats []model.TableFormat `json:"targetFormats" yaml:"targetFormats"`
	// TableBasePath is the root directory path of the table.
	TableBasePath string `json:"tableBasePath" yaml:"tableBasePath"`
	// TableDataPath is the optional data directory path (defaults to TableBasePath).
	TableDataPath string `json:"tableDataPath,omitempty" yaml:"tableDataPath,omitempty"`
	// TableName is the logical table name.
	TableName string `json:"tableName,omitempty" yaml:"tableName,omitempty"`
	// Namespace is an optional database/catalog namespace (e.g. "default.sales").
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// SyncMode controls FULL snapshot sync vs INCREMENTAL sync.
	SyncMode spi.SyncMode `json:"syncMode,omitempty" yaml:"syncMode,omitempty"`
}

// Validate validates that required dataset configuration parameters are provided.
func (c *DatasetConfig) Validate() error {
	if c.SourceFormat == "" {
		return fmt.Errorf("sourceFormat is required")
	}
	if len(c.TargetFormats) == 0 {
		return fmt.Errorf("at least one targetFormat must be specified")
	}
	if c.TableBasePath == "" {
		return fmt.Errorf("tableBasePath is required")
	}
	if c.SyncMode == "" {
		c.SyncMode = spi.SyncModeIncremental
	}
	return nil
}

// Config represents top-level YAML / JSON configuration file containing multiple datasets.
type Config struct {
	SourceFormat  model.TableFormat   `json:"sourceFormat,omitempty" yaml:"sourceFormat,omitempty"`
	TargetFormats []model.TableFormat `json:"targetFormats,omitempty" yaml:"targetFormats,omitempty"`
	Datasets      []*DatasetConfig    `json:"datasets" yaml:"datasets"`
}
