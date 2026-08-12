//go:build js && wasm

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
	"syscall/js"

	"github.com/apache/incubator-xtable-go/pkg/conversion"
	"github.com/apache/incubator-xtable-go/pkg/formats"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/model"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

func main() {
	js.Global().Set("xtableVersion", js.FuncOf(func(this js.Value, args []js.Value) any {
		return "0.1.0-WASM"
	}))

	js.Global().Set("xtableInspect", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 2 {
			return makeErrorResult("arguments 'format' and 'basePath' are required")
		}

		formatStr := args[0].String()
		basePath := args[1].String()

		format, err := model.ParseTableFormat(formatStr)
		if err != nil {
			return makeErrorResult(err.Error())
		}

		ctx := context.Background()
		storage, err := io.NewStorageForPath(ctx, basePath)
		if err != nil {
			return makeErrorResult(fmt.Sprintf("storage error: %v", err))
		}

		source, err := formats.NewSource(format, storage, basePath)
		if err != nil {
			return makeErrorResult(fmt.Sprintf("failed to create format source: %v", err))
		}

		table, err := source.GetCurrentTable(ctx)
		if err != nil {
			return makeErrorResult(fmt.Sprintf("metadata error: %v", err))
		}

		snap, err := source.GetCurrentSnapshot(ctx)
		if err != nil {
			return makeErrorResult(fmt.Sprintf("snapshot error: %v", err))
		}

		var partFields []string
		for _, pf := range table.PartitioningFields {
			partFields = append(partFields, pf.SourceField.Name)
		}

		resp := map[string]any{
			"status":               "SUCCESS",
			"tableName":            table.Name,
			"format":               table.TableFormat,
			"basePath":             table.BasePath,
			"latestCommitTime":     table.LatestCommitTime,
			"activeDataFilesCount": len(snap.DataFiles),
			"partitionFields":      partFields,
			"schema":               table.ReadSchema,
		}

		outBytes, _ := json.Marshal(resp)
		return string(outBytes)
	}))

	js.Global().Set("xtableSync", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 1 {
			return makeErrorResult("configuration JSON string is required")
		}

		configJSON := args[0].String()
		var cfg conversion.Config
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return makeErrorResult(fmt.Sprintf("JSON parse error: %v", err))
		}

		ctx := context.Background()
		allResults := make(map[string]map[model.TableFormat]*spi.SyncResult)

		for _, ds := range cfg.Datasets {
			if ds.SourceFormat == "" && cfg.SourceFormat != "" {
				ds.SourceFormat = cfg.SourceFormat
			}
			if len(ds.TargetFormats) == 0 && len(cfg.TargetFormats) > 0 {
				ds.TargetFormats = cfg.TargetFormats
			}

			storage, sErr := io.NewStorageForPath(ctx, ds.TableBasePath)
			if sErr != nil {
				return makeErrorResult(fmt.Sprintf("storage error for %s: %v", ds.TableBasePath, sErr))
			}

			controller := conversion.NewController(storage)
			res, syncErr := controller.Sync(ctx, ds)
			if syncErr != nil {
				return makeErrorResult(fmt.Sprintf("sync error for %s: %v", ds.TableName, syncErr))
			}
			allResults[ds.TableName] = res
		}

		outBytes, _ := json.Marshal(map[string]any{
			"status":  "SUCCESS",
			"results": allResults,
		})
		return string(outBytes)
	}))

	// Keep WebAssembly event loop running
	select {}
}

func makeErrorResult(msg string) string {
	outBytes, _ := json.Marshal(map[string]any{
		"status": "ERROR",
		"error":  msg,
	})
	return string(outBytes)
}
