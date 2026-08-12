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

package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/incubator-xtable-go/pkg/model"
)

// IcebergRESTConversionSource resolves tables registered in an Iceberg REST Catalog
// (Polaris, Unity, Tabular, Nessie).
type IcebergRESTConversionSource struct {
	httpClient *http.Client
	baseURI    string
	namespace  string
	authToken  string
}

var _ ConversionSource = (*IcebergRESTConversionSource)(nil)

// loadTableResponse is the subset of the Iceberg REST LoadTableResult this source reads.
type loadTableResponse struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		Location   string            `json:"location"`
		Properties map[string]string `json:"properties"`
	} `json:"metadata"`
}

// NewIcebergRESTConversionSource creates a conversion source for an Iceberg REST Catalog.
func NewIcebergRESTConversionSource(cfg *Config) (*IcebergRESTConversionSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.URI == "" {
		return nil, fmt.Errorf("URI is required for Iceberg REST catalog")
	}

	token := ""
	if cfg.Properties != nil {
		token = cfg.Properties["token"]
	}

	return &IcebergRESTConversionSource{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURI:    strings.TrimSuffix(cfg.URI, "/"),
		namespace:  cfg.DatabaseName,
		authToken:  token,
	}, nil
}

// NewIcebergRESTConversionSourceWithClient creates a source using a caller-supplied HTTP client.
func NewIcebergRESTConversionSourceWithClient(client *http.Client, baseURI, namespace, authToken string) *IcebergRESTConversionSource {
	return &IcebergRESTConversionSource{
		httpClient: client,
		baseURI:    strings.TrimSuffix(baseURI, "/"),
		namespace:  namespace,
		authToken:  authToken,
	}
}

// CatalogType returns ICEBERG_REST.
func (c *IcebergRESTConversionSource) CatalogType() CatalogType {
	return CatalogTypeIcebergREST
}

// GetSourceTable loads a table from the REST catalog and resolves it to a SourceTable. An Iceberg
// REST catalog only ever serves Iceberg tables, so the format is not inferred from properties.
func (c *IcebergRESTConversionSource) GetSourceTable(ctx context.Context, id TableIdentifier) (*SourceTable, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s",
		c.baseURI, url.PathEscape(id.Database), url.PathEscape(id.Table))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build load-table request for %s: %w", id, err)
	}
	req.Header.Set("Accept", "application/json")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to load table %s from Iceberg REST catalog: %w", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("table %s not found in Iceberg REST catalog", id)
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("iceberg REST catalog returned %d loading table %s: %s", resp.StatusCode, id, strings.TrimSpace(string(body)))
	}

	var loaded loadTableResponse
	if err := json.NewDecoder(resp.Body).Decode(&loaded); err != nil {
		return nil, fmt.Errorf("failed to decode load-table response for %s: %w", id, err)
	}

	basePath := strings.TrimSpace(loaded.Metadata.Location)
	if basePath == "" {
		// Fall back to the metadata file's location, trimming /metadata/<file>.
		basePath = baseFromMetadataLocation(loaded.MetadataLocation)
	}
	if basePath == "" {
		return nil, fmt.Errorf("table %s carries no location in the REST catalog response", id)
	}

	dataPath, err := DataLocationForFormat(model.TableFormatIceberg, basePath, loaded.Metadata.Properties)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", id, err)
	}

	properties := make(map[string]string, len(loaded.Metadata.Properties)+1)
	for k, v := range loaded.Metadata.Properties {
		properties[k] = v
	}
	properties[PropTableType] = string(model.TableFormatIceberg)

	return &SourceTable{
		Name:       id.Table,
		BasePath:   basePath,
		DataPath:   dataPath,
		Format:     model.TableFormatIceberg,
		Properties: properties,
	}, nil
}

// baseFromMetadataLocation derives a table root from a metadata file path of the shape
// <basePath>/metadata/<file>.metadata.json. It returns "" when the shape does not match.
func baseFromMetadataLocation(metadataLocation string) string {
	metadataLocation = strings.TrimSpace(metadataLocation)
	if metadataLocation == "" {
		return ""
	}
	idx := strings.LastIndex(metadataLocation, "/metadata/")
	if idx <= 0 {
		return ""
	}
	return metadataLocation[:idx]
}

// Close releases any network connections.
func (c *IcebergRESTConversionSource) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
