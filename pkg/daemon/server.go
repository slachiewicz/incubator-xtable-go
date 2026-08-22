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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Server implements the HTTP REST server for polytable matching the OpenAPI spec.
type Server struct {
	mux         *http.ServeMux
	conversions map[string]*ConvertTableResponse
	mu          sync.RWMutex
	version     string
	storage     io.Storage
}

// NewServer creates a new REST API Server.
func NewServer(version string) *Server {
	return NewServerWithStorage(version, nil)
}

// NewServerWithStorage creates a new REST API Server with custom storage.
func NewServerWithStorage(version string, storage io.Storage) *Server {
	if version == "" {
		version = "0.1.0-SNAPSHOT"
	}
	s := &Server{
		mux:         http.NewServeMux(),
		conversions: make(map[string]*ConvertTableResponse),
		version:     version,
		storage:     storage,
	}
	s.registerRoutes()
	return s
}

func (s *Server) getStorage(ctx context.Context, path string, storageConfig *conversion.StorageConfig) (io.Storage, error) {
	if s.storage != nil {
		return s.storage, nil
	}
	if storageConfig != nil {
		return io.NewStorageForPathWithOptions(ctx, path, storageConfig.ToOptionFuncs()...)
	}
	return io.NewStorageForPath(ctx, path)
}

// Handler returns the http.Handler for the server.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/v1/health", s.handleHealth)
	s.mux.HandleFunc("/v1/conversion/table", s.handleConvertTable)
	s.mux.HandleFunc("/v1/conversion/table/", s.handleGetConversionStatus)
	s.mux.HandleFunc("/v1/conversion/inspect", s.handleInspectTable)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	resp := HealthStatus{
		Status:    "UP",
		Version:   s.version,
		Timestamp: time.Now(),
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleConvertTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ConvertTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON payload: %v", err), http.StatusBadRequest)
		return
	}

	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  req.SourceFormat,
		TargetFormats: req.TargetFormats,
		TableName:     req.TableName,
		TableBasePath: req.TableBasePath,
		TableDataPath: req.TableDataPath,
		Namespace:     req.Namespace,
		SyncMode:      req.SyncMode,
		Storage:       req.Storage,
	}

	conversionID := uuid.New().String()
	preferAsync := r.Header.Get("Prefer") == "respond-async"

	if preferAsync {
		// Asynchronous processing
		resp := &ConvertTableResponse{
			ConversionID: conversionID,
			Status:       "RUNNING",
			SubmittedAt:  time.Now(),
		}
		s.mu.Lock()
		s.conversions[conversionID] = resp
		s.mu.Unlock()

		// The conversion outlives this request, so cancellation is detached from
		// r.Context() while request-scoped values are preserved.
		bgCtx := context.WithoutCancel(r.Context())

		go func() {
			storage, err := s.getStorage(bgCtx, datasetConfig.TableBasePath, datasetConfig.Storage)
			if err != nil {
				s.recordFailure(conversionID, err)
				return
			}
			controller := conversion.NewController(storage)
			results, err := controller.Sync(bgCtx, datasetConfig)
			if err != nil {
				s.recordFailure(conversionID, err)
				return
			}
			now := time.Now()
			s.mu.Lock()
			if conv, ok := s.conversions[conversionID]; ok {
				conv.Status = "COMPLETED"
				conv.FinishedAt = &now
				conv.Results = results
			}
			s.mu.Unlock()
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		snapshot := cloneConvertTableResponse(resp)
		_ = json.NewEncoder(w).Encode(&snapshot)
		return
	}

	// Synchronous processing
	storage, err := s.getStorage(r.Context(), datasetConfig.TableBasePath, datasetConfig.Storage)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to init storage: %v", err), http.StatusInternalServerError)
		return
	}

	controller := conversion.NewController(storage)
	results, err := controller.Sync(r.Context(), datasetConfig)
	now := time.Now()

	resp := ConvertTableResponse{
		ConversionID: conversionID,
		Status:       "COMPLETED",
		SubmittedAt:  now,
		FinishedAt:   &now,
		Results:      results,
	}

	if err != nil {
		resp.Status = "FAILED"
		resp.Error = err.Error()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGetConversionStatus(w http.ResponseWriter, r *http.Request) {
	conversionID := strings.TrimPrefix(r.URL.Path, "/v1/conversion/table/")
	if conversionID == "" {
		http.Error(w, "missing conversionId", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	resp, ok := s.conversions[conversionID]
	if !ok {
		s.mu.RUnlock()
		http.Error(w, "conversion job not found", http.StatusNotFound)
		return
	}

	// Snapshot under the lock so the background goroutine cannot mutate
	// fields (Status, Results, FinishedAt) while we JSON-encode them.
	snapshot := cloneConvertTableResponse(resp)
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if snapshot.Status == "RUNNING" {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(&snapshot)
}

func (s *Server) handleInspectTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req InspectTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	storage, err := s.getStorage(r.Context(), req.TableBasePath, req.Storage)
	if err != nil {
		http.Error(w, fmt.Sprintf("storage error: %v", err), http.StatusInternalServerError)
		return
	}

	source, err := formats.NewSource(req.Format, storage, req.TableBasePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create format source: %v", err), http.StatusBadRequest)
		return
	}

	table, err := source.GetCurrentTable(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read table metadata: %v", err), http.StatusInternalServerError)
		return
	}

	snap, err := source.GetCurrentSnapshot(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read table snapshot: %v", err), http.StatusInternalServerError)
		return
	}

	var partFields []string
	for _, pf := range table.PartitioningFields {
		partFields = append(partFields, pf.SourceField.Name)
	}

	resp := InspectTableResponse{
		TableName:            table.Name,
		Format:               table.TableFormat,
		TableBasePath:        table.BasePath,
		LatestCommitTime:     table.LatestCommitTime,
		ActiveDataFilesCount: len(snap.DataFiles),
		PartitionFields:      partFields,
		Schema:               table.ReadSchema,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) recordFailure(id string, err error) {
	now := time.Now()
	s.mu.Lock()
	if conv, ok := s.conversions[id]; ok {
		conv.Status = "FAILED"
		conv.FinishedAt = &now
		conv.Error = err.Error()
	}
	s.mu.Unlock()
}

func cloneConvertTableResponse(resp *ConvertTableResponse) ConvertTableResponse {
	if resp == nil {
		return ConvertTableResponse{}
	}

	snapshot := *resp
	if resp.FinishedAt != nil {
		finishedAt := *resp.FinishedAt
		snapshot.FinishedAt = &finishedAt
	}
	if len(resp.Results) == 0 {
		return snapshot
	}

	snapshot.Results = make(map[model.TableFormat]*spi.SyncResult, len(resp.Results))
	for format, result := range resp.Results {
		if result == nil {
			snapshot.Results[format] = nil
			continue
		}
		// spi.SyncResult currently contains only value fields, so a shallow copy
		// is sufficient to detach the response snapshot from later mutations.
		clonedResult := *result
		snapshot.Results[format] = &clonedResult
	}

	return snapshot
}
