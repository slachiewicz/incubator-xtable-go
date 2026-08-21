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
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	xtio "github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

var (
	version = "0.1.0-SNAPSHOT"
	commit  = "none"
	date    = "unknown"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "polytable",
		Short: "polytable: cross-format lakehouse metadata converter (unofficial Go port of Apache XTable)",
		Long: `polytable provides omni-directional, zero-copy metadata translation
across Apache Iceberg, Delta Lake, Apache Hudi, and other open lakehouse table formats.`,
		// Wires up --version, which people reach for before discovering `polytable version`.
		Version:      fmt.Sprintf("%s (commit: %s, date: %s)", version, commit, date),
		SilenceUsage: true,
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
		Run: func(cmd *cobra.Command, _ []string) {
			// Same string as --version, so the two never drift apart.
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "polytable version %s (commit: %s, date: %s)\n", version, commit, date)
		},
	}
}

// parseOutputFormat validates the --output flag. "" and "text" both mean human-readable output;
// "json" switches to the machine-readable document described in cmd/polytable/output.go.
func parseOutputFormat(s string) (isJSON bool, err error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "text":
		return false, nil
	case "json":
		return true, nil
	default:
		return false, fmt.Errorf("--output must be %q or %q, got %q", "text", "json", s)
	}
}

// parseSyncModeFlag validates and translates the --mode flag. An empty string means "leave
// DatasetConfig.SyncMode as the config file set it".
func parseSyncModeFlag(s string) (spi.SyncMode, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "":
		return "", nil
	case "FULL":
		return spi.SyncModeFull, nil
	case "INCREMENTAL":
		return spi.SyncModeIncremental, nil
	default:
		return "", fmt.Errorf("--mode must be %q or %q, got %q", "full", "incremental", s)
	}
}

func newSyncCmd() *cobra.Command {
	var (
		configPath string
		outputStr  string
		modeStr    string
		dryRun     bool
		timeout    time.Duration
	)

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Synchronize table formats using a dataset configuration file",
		Example: `  polytable sync --datasetConfig config.yaml
  polytable sync -c my_dataset.json
  polytable sync -c config.yaml --output json --dry-run
  polytable sync -c config.yaml --mode full --timeout 5m`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if configPath == "" {
				return fmt.Errorf("--datasetConfig flag is required")
			}

			outputJSON, err := parseOutputFormat(outputStr)
			if err != nil {
				return err
			}
			modeOverride, err := parseSyncModeFlag(modeStr)
			if err != nil {
				return err
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

			// If top-level sourceFormat and targetFormats are set, propagate to datasets. Apply
			// --mode here too, so every dataset sees the same override regardless of what its
			// individual config entry said.
			for _, ds := range cfg.Datasets {
				if ds.SourceFormat == "" && cfg.SourceFormat != "" {
					ds.SourceFormat = cfg.SourceFormat
				}
				if len(ds.TargetFormats) == 0 && len(cfg.TargetFormats) > 0 {
					ds.TargetFormats = cfg.TargetFormats
				}
				if modeOverride != "" {
					ds.SyncMode = modeOverride
				}
			}

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			// Progress chatter always goes to stderr when emitting JSON, so stdout carries only
			// the machine-readable document. In text mode both go to stdout, matching prior
			// behavior.
			stdout := cmd.OutOrStdout()
			progress := stdout
			if outputJSON {
				progress = cmd.ErrOrStderr()
			}

			_, _ = fmt.Fprintln(progress, "🚀 Starting polytable synchronization...")
			overallStart := time.Now()

			out := SyncOutput{StartedAt: overallStart, DryRun: dryRun}
			hasErrors := false

			for i, ds := range cfg.Datasets {
				_, _ = fmt.Fprintf(progress, "\n[%d/%d] Syncing Table '%s' (%s -> %v)...\n",
					i+1, len(cfg.Datasets), ds.TableName, ds.SourceFormat, ds.TargetFormats)

				tableOut := syncOneDataset(ctx, ds, dryRun, timeout, progress)
				if tableOut.hasFailure() {
					hasErrors = true
				}
				out.Tables = append(out.Tables, tableOut)
			}

			out.Duration = time.Since(overallStart)
			out.HasErrors = hasErrors

			_, _ = fmt.Fprintf(progress, "\n✨ Finished all dataset syncs in %v\n", out.Duration)

			if outputJSON {
				enc := json.NewEncoder(stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(out); err != nil {
					return fmt.Errorf("failed to encode JSON output: %w", err)
				}
			}

			if hasErrors {
				return fmt.Errorf("one or more table syncs failed")
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "datasetConfig", "c", "", "Path to YAML/JSON dataset config file")
	cmd.Flags().StringVar(&configPath, "config", "", "Alias for --datasetConfig")
	cmd.Flags().StringVarP(&outputStr, "output", "o", "text", `Output format: "text" or "json"`)
	cmd.Flags().StringVar(&modeStr, "mode", "", `Override DatasetConfig.SyncMode: "full" or "incremental"`)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Resolve the source and compute changes without committing them")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Per-table timeout (e.g. 30s, 5m); 0 means no timeout")
	return cmd
}

// withDatasetTimeout wraps ctx in a context.WithTimeout when timeout > 0. The caller must always
// call the returned cancel, even when it is a no-op. Extracted from syncOneDataset so the timeout
// wiring itself is unit-testable without needing storage slow enough to observe a real deadline.
func withDatasetTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// syncOneDataset resolves, syncs, and reports on a single dataset, applying timeout as a
// context.WithTimeout around exactly this dataset's work.
func syncOneDataset(ctx context.Context, ds *conversion.DatasetConfig, dryRun bool, timeout time.Duration, progress io.Writer) TableSyncOutput {
	datasetCtx, cancel := withDatasetTimeout(ctx, timeout)
	defer cancel()

	// Resolve a catalog-addressed table (db.table) into a base path first: the storage backend is
	// chosen from that path, so this cannot wait until after.
	if rErr := conversion.ResolveSourceCatalog(datasetCtx, ds, nil); rErr != nil {
		_, _ = fmt.Fprintf(progress, "  ❌ Failed to resolve source catalog: %v\n", rErr)
		return buildTableSyncOutput(ds, nil, rErr)
	}

	optFns := ds.Storage.ToS3OptionFuncs()
	storage, err := xtio.NewStorageForPathWithOptions(datasetCtx, ds.TableBasePath, optFns...)
	if err != nil {
		_, _ = fmt.Fprintf(progress, "  ❌ Failed to initialize storage for %s: %v\n", ds.TableBasePath, err)
		return buildTableSyncOutput(ds, nil, err)
	}

	controller := conversion.NewController(storage)
	var syncOpts []conversion.SyncOption
	if dryRun {
		syncOpts = append(syncOpts, conversion.WithDryRun())
	}

	results, err := controller.Sync(datasetCtx, ds, syncOpts...)
	if err != nil {
		_, _ = fmt.Fprintf(progress, "  ❌ Failed: %v\n", err)
		return buildTableSyncOutput(ds, nil, err)
	}

	tableOut := buildTableSyncOutput(ds, results, nil)
	for _, t := range tableOut.Targets {
		switch t.Verdict {
		case string(spi.SyncVerdictSuccess):
			_, _ = fmt.Fprintf(progress, "  ✅ [%s] Synced successfully in %v (lastInstant: %d)\n", t.TargetFormat, t.Duration, t.LastInstantSynced)
		case string(spi.SyncVerdictNoOp):
			_, _ = fmt.Fprintf(progress, "  ⏭  [%s] No new commits to sync (in %v)\n", t.TargetFormat, t.Duration)
		default:
			_, _ = fmt.Fprintf(progress, "  ❌ [%s] Sync error: %s (in %v)\n", t.TargetFormat, t.Error, t.Duration)
		}
	}
	return tableOut
}

func newInspectCmd() *cobra.Command {
	var basePath string
	var formatStr string
	var outputStr string

	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect table schema and metadata",
		Example: `  polytable inspect --basePath ./my_table --format DELTA
  polytable inspect --basePath ./my_table --format ICEBERG
  polytable inspect --basePath ./my_parquet_data --format PARQUET
  polytable inspect --basePath ./my_table --format DELTA --output json`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if basePath == "" {
				return fmt.Errorf("--basePath flag is required")
			}
			outputJSON, err := parseOutputFormat(outputStr)
			if err != nil {
				return err
			}
			format, err := model.ParseTableFormat(formatStr)
			if err != nil {
				return err
			}

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			storage, err := xtio.NewStorageForPath(ctx, basePath)
			if err != nil {
				return fmt.Errorf("failed to initialize storage for %s: %w", basePath, err)
			}

			source, err := formats.NewSource(format, storage, basePath)
			if err != nil {
				return fmt.Errorf("failed to create format source: %w", err)
			}

			table, err := source.GetCurrentTable(ctx)
			if err != nil {
				return fmt.Errorf("failed to read table metadata: %w", err)
			}

			snapshot, err := source.GetCurrentSnapshot(ctx)
			if err != nil {
				return fmt.Errorf("failed to read table snapshot: %w", err)
			}

			if outputJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(buildInspectOutput(table, snapshot))
			}

			stdout := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(stdout, "📊 Table: %s (Format: %s)\n", table.Name, table.TableFormat)
			_, _ = fmt.Fprintf(stdout, "📍 Base Path: %s\n", table.BasePath)
			_, _ = fmt.Fprintf(stdout, "🕒 Latest Commit Time: %s\n", time.UnixMilli(table.LatestCommitTime).Format(time.RFC3339))
			_, _ = fmt.Fprintf(stdout, "📁 Active Data Files: %d\n", len(snapshot.DataFiles))

			_, _ = fmt.Fprintln(stdout, "\n📋 Schema:")
			for _, f := range table.ReadSchema.Fields {
				nullStr := "nullable"
				if !f.Schema.IsNullable {
					nullStr = "required"
				}
				_, _ = fmt.Fprintf(stdout, "  - %s: %s (%s)\n", f.Name, f.Schema.DataType, nullStr)
			}

			if len(table.PartitioningFields) > 0 {
				_, _ = fmt.Fprintln(stdout, "\n🗂 Partition Fields:")
				for _, pf := range table.PartitioningFields {
					_, _ = fmt.Fprintf(stdout, "  - %s (transform: %s)\n", pf.SourceField.Name, pf.TransformType)
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&basePath, "basePath", "b", "", "Root directory of the table")
	cmd.Flags().StringVar(&basePath, "path", "", "Alias for --basePath")
	cmd.Flags().StringVarP(&formatStr, "format", "f", "", "Table format (DELTA, ICEBERG, HUDI, PARQUET, PAIMON)")
	cmd.Flags().StringVarP(&outputStr, "output", "o", "text", `Output format: "text" or "json"`)
	return cmd
}
