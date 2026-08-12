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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/apache/incubator-xtable-go/pkg/conversion"
	"github.com/apache/incubator-xtable-go/pkg/formats/delta"
	"github.com/apache/incubator-xtable-go/pkg/formats/hudi"
	"github.com/apache/incubator-xtable-go/pkg/formats/iceberg"
	"github.com/apache/incubator-xtable-go/pkg/formats/parquet"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

var (
	version = "0.1.0-SNAPSHOT"
	commit  = "none"
	date    = "unknown"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "xtable",
		Short: "Apache XTable (Go): Cross-table format converter for modern lakehouses",
		Long: `Apache XTable (Go) provides omni-directional, zero-copy metadata translation
across Apache Iceberg, Delta Lake, Apache Hudi, and other open lakehouse table formats.`,
	}

	rootCmd.AddCommand(newSyncCmd())
	rootCmd.AddCommand(newInspectCmd())
	rootCmd.AddCommand(newVersionCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("xtable-go version %s (commit: %s, date: %s)\n", version, commit, date)
		},
	}
}

func newSyncCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Synchronize table formats using a dataset configuration file",
		Example: `  xtable sync --datasetConfig config.yaml
  xtable sync -c my_dataset.json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if configPath == "" {
				return fmt.Errorf("--datasetConfig flag is required")
			}

			data, err := os.ReadFile(configPath)
			if err != nil {
				return fmt.Errorf("failed to read config file %s: %w", configPath, err)
			}

			var cfg conversion.Config
			if strings.HasSuffix(configPath, ".json") {
				if err := json.Unmarshal(data, &cfg); err != nil {
					return fmt.Errorf("failed to parse JSON config: %w", err)
				}
			} else {
				if err := yaml.Unmarshal(data, &cfg); err != nil {
					return fmt.Errorf("failed to parse YAML config: %w", err)
				}
			}

			// If top-level sourceFormat and targetFormats are set, propagate to datasets
			for _, ds := range cfg.Datasets {
				if ds.SourceFormat == "" && cfg.SourceFormat != "" {
					ds.SourceFormat = cfg.SourceFormat
				}
				if len(ds.TargetFormats) == 0 && len(cfg.TargetFormats) > 0 {
					ds.TargetFormats = cfg.TargetFormats
				}
			}

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			fmt.Println("🚀 Starting Apache XTable Synchronization...")
			overallStart := time.Now()

			hasErrors := false
			for i, ds := range cfg.Datasets {
				fmt.Printf("\n[%d/%d] Syncing Table '%s' (%s -> %v)...\n",
					i+1, len(cfg.Datasets), ds.TableName, ds.SourceFormat, ds.TargetFormats)

				storage, err := io.NewStorageForPath(ctx, ds.TableBasePath)
				if err != nil {
					fmt.Printf("  ❌ Failed to initialize storage for %s: %v\n", ds.TableBasePath, err)
					hasErrors = true
					continue
				}

				controller := conversion.NewController(storage)
				results, err := controller.Sync(ctx, ds)
				if err != nil {
					fmt.Printf("  ❌ Failed: %v\n", err)
					hasErrors = true
					continue
				}

				for targetFormat, res := range results {
					if res.StatusCode == spi.SyncStatusSuccess {
						fmt.Printf("  ✅ [%s] Synced successfully in %v (lastInstant: %d)\n",
							targetFormat, res.Duration, res.LastInstantSynced)
					} else {
						fmt.Printf("  ❌ [%s] Sync error: %s (in %v)\n",
							targetFormat, res.Error, res.Duration)
						hasErrors = true
					}
				}
			}

			fmt.Printf("\n✨ Finished all dataset syncs in %v\n", time.Since(overallStart))
			if hasErrors {
				return fmt.Errorf("one or more table syncs failed")
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "datasetConfig", "c", "", "Path to YAML/JSON dataset config file")
	return cmd
}

func newInspectCmd() *cobra.Command {
	var basePath string
	var formatStr string

	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect table schema and metadata",
		Example: `  xtable inspect --basePath ./my_table --format DELTA
  xtable inspect --basePath ./my_table --format ICEBERG
  xtable inspect --basePath ./my_parquet_data --format PARQUET`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if basePath == "" {
				return fmt.Errorf("--basePath flag is required")
			}
			format, err := model.ParseTableFormat(formatStr)
			if err != nil {
				return err
			}

			ctx := context.Background()
			storage, err := io.NewStorageForPath(ctx, basePath)
			if err != nil {
				return fmt.Errorf("failed to initialize storage for %s: %w", basePath, err)
			}

			var source spi.ConversionSource
			switch format {
			case model.TableFormatDelta:
				source = delta.NewSource(storage, basePath)
			case model.TableFormatIceberg:
				source = iceberg.NewSource(storage, basePath)
			case model.TableFormatHudi:
				source = hudi.NewSource(storage, basePath)
			case model.TableFormatParquet:
				source = parquet.NewSource(storage, basePath)
			default:
				return fmt.Errorf("unsupported inspect format: %s", format)
			}

			table, err := source.GetCurrentTable(ctx)
			if err != nil {
				return fmt.Errorf("failed to read table metadata: %w", err)
			}

			snapshot, err := source.GetCurrentSnapshot(ctx)
			if err != nil {
				return fmt.Errorf("failed to read table snapshot: %w", err)
			}

			fmt.Printf("📊 Table: %s (Format: %s)\n", table.Name, table.TableFormat)
			fmt.Printf("📍 Base Path: %s\n", table.BasePath)
			fmt.Printf("🕒 Latest Commit Time: %s\n", time.UnixMilli(table.LatestCommitTime).Format(time.RFC3339))
			fmt.Printf("📁 Active Data Files: %d\n", len(snapshot.DataFiles))

			fmt.Println("\n📋 Schema:")
			for _, f := range table.ReadSchema.Fields {
				nullStr := "nullable"
				if !f.Schema.IsNullable {
					nullStr = "required"
				}
				fmt.Printf("  - %s: %s (%s)\n", f.Name, f.Schema.DataType, nullStr)
			}

			if len(table.PartitioningFields) > 0 {
				fmt.Println("\n🗂 Partition Fields:")
				for _, pf := range table.PartitioningFields {
					fmt.Printf("  - %s (transform: %s)\n", pf.SourceField.Name, pf.TransformType)
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&basePath, "basePath", "b", "", "Root directory of the table")
	cmd.Flags().StringVarP(&formatStr, "format", "f", "", "Table format (DELTA, ICEBERG)")
	return cmd
}
