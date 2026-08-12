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

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unsafe"

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

//export xtable_version
func xtable_version() *C.char {
	return C.CString("0.1.0-SNAPSHOT")
}

//export xtable_free_string
func xtable_free_string(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

//export xtable_sync_json
func xtable_sync_json(configStr *C.char) *C.char {
	if configStr == nil {
		return errorJSON("config string is null")
	}

	goStr := C.GoString(configStr)
	var cfg conversion.Config

	var err error
	if strings.HasPrefix(strings.TrimSpace(goStr), "{") {
		err = json.Unmarshal([]byte(goStr), &cfg)
	} else {
		err = yaml.Unmarshal([]byte(goStr), &cfg)
	}
	if err != nil {
		return errorJSON(fmt.Sprintf("failed to parse configuration: %v", err))
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
			return errorJSON(fmt.Sprintf("failed to initialize storage for %s: %v", ds.TableBasePath, sErr))
		}

		controller := conversion.NewController(storage)
		res, syncErr := controller.Sync(ctx, ds)
		if syncErr != nil {
			return errorJSON(fmt.Sprintf("sync error for table %s: %v", ds.TableName, syncErr))
		}
		allResults[ds.TableName] = res
	}

	outBytes, _ := json.Marshal(map[string]any{
		"status":  "SUCCESS",
		"results": allResults,
	})
	return C.CString(string(outBytes))
}

//export xtable_inspect_json
func xtable_inspect_json(formatCStr *C.char, basePathCStr *C.char) *C.char {
	if formatCStr == nil || basePathCStr == nil {
		return errorJSON("format and basePath arguments are required")
	}

	formatStr := C.GoString(formatCStr)
	basePath := C.GoString(basePathCStr)

	format, err := model.ParseTableFormat(formatStr)
	if err != nil {
		return errorJSON(err.Error())
	}

	ctx := context.Background()
	storage, err := io.NewStorageForPath(ctx, basePath)
	if err != nil {
		return errorJSON(fmt.Sprintf("storage error: %v", err))
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
		return errorJSON(fmt.Sprintf("unsupported format: %s", format))
	}

	table, err := source.GetCurrentTable(ctx)
	if err != nil {
		return errorJSON(fmt.Sprintf("failed to read table metadata: %v", err))
	}

	snap, err := source.GetCurrentSnapshot(ctx)
	if err != nil {
		return errorJSON(fmt.Sprintf("failed to read snapshot: %v", err))
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
	return C.CString(string(outBytes))
}

func errorJSON(msg string) *C.char {
	outBytes, _ := json.Marshal(map[string]any{
		"status": "ERROR",
		"error":  msg,
	})
	return C.CString(string(outBytes))
}

func main() {}
