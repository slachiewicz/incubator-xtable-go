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
	"iter"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/slachiewicz/polytable/pkg/model"
)

// IcebergRESTConversionSource resolves tables registered in an Iceberg REST Catalog
// (Polaris, Unity, Tabular, Nessie).
type IcebergRESTConversionSource struct {
	endpoint  *restCatalogEndpoint
	namespace string
}

var _ ConversionSource = (*IcebergRESTConversionSource)(nil)

// loadTableResponse is the subset of the Iceberg REST LoadTableResult this source reads. Config and
// StorageCredentials are only populated when the request carried AccessDelegationHeader and the
// catalog supports it; a catalog that does not vend credentials leaves both empty, which
// parseStorageCredentials turns into a nil *StorageCredentials rather than an error.
type loadTableResponse struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		Location   string            `json:"location"`
		Properties map[string]string `json:"properties"`
	} `json:"metadata"`
	// Config is the older, flat shape for vended credentials -- observed against a live Snowflake
	// catalog on 2026-08-22, carrying s3.access-key-id, s3.secret-access-key, s3.session-token,
	// expiration-time and client.region.
	Config map[string]string `json:"config"`
	// StorageCredentials is the current Iceberg REST specification's shape: a list of credential
	// sets, each scoped to a prefix, so a catalog can vend different credentials for different
	// parts of a table (or a warehouse). Not observed against a real server; this code accepts it
	// on the strength of the specification alone and prefers it over Config when both are present.
	StorageCredentials []loadTableStorageCredential `json:"storage-credentials"`
}

// loadTableStorageCredential is one entry of loadTableResponse.StorageCredentials.
type loadTableStorageCredential struct {
	// Prefix scopes this credential set to locations starting with it. An empty prefix applies to
	// every location the response's table (or warehouse) can address.
	Prefix string `json:"prefix"`
	// Config carries the same key names as loadTableResponse.Config, scoped to Prefix.
	Config map[string]string `json:"config"`
}

// listNamespacesResponse is the subset of GET /v1/{prefix}/namespaces this source reads.
type listNamespacesResponse struct {
	Namespaces    [][]string `json:"namespaces"`
	NextPageToken string     `json:"next-page-token"`
}

// listTablesResponse is the subset of GET /v1/{prefix}/namespaces/{namespace}/tables this source
// reads.
type listTablesResponse struct {
	Identifiers []struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	} `json:"identifiers"`
	NextPageToken string `json:"next-page-token"`
}

// nestedNamespaceSeparator is the unit separator the Iceberg REST specification uses to join a
// multi-level namespace's parts into a single path segment (e.g. "a\x1Fb" for namespace ["a","b"]).
const nestedNamespaceSeparator = "\x1F"

// NewIcebergRESTConversionSource creates a conversion source for an Iceberg REST Catalog.
func NewIcebergRESTConversionSource(cfg *Config) (*IcebergRESTConversionSource, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.URI == "" {
		return nil, fmt.Errorf("URI is required for Iceberg REST catalog")
	}

	client, token, err := restHTTPClient(cfg, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return &IcebergRESTConversionSource{
		endpoint:  newRESTCatalogEndpoint(client, cfg.URI, token, cfg.Properties),
		namespace: cfg.DatabaseName,
	}, nil
}

// NewIcebergRESTConversionSourceWithClient creates a source using a caller-supplied HTTP client. It
// carries no warehouse: callers needing prefix negotiation with a warehouse go through
// NewIcebergRESTConversionSource.
func NewIcebergRESTConversionSourceWithClient(client *http.Client, baseURI, namespace, authToken string) *IcebergRESTConversionSource {
	return &IcebergRESTConversionSource{
		endpoint:  newRESTCatalogEndpoint(client, baseURI, authToken, nil),
		namespace: namespace,
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
	if err := c.endpoint.negotiatePrefix(ctx); err != nil {
		return nil, fmt.Errorf("failed to negotiate Iceberg REST catalog config: %w", err)
	}

	endpoint := c.endpoint.path("namespaces", id.Database, "tables", id.Table)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build load-table request for %s: %w", id, err)
	}
	req.Header.Set("Accept", "application/json")
	// Ask for scoped storage credentials alongside the table's metadata. A catalog that does not
	// support delegation (the common case today) simply ignores the header and returns the same
	// response it always has: parseStorageCredentials below turns the resulting empty Config and
	// StorageCredentials into a nil *StorageCredentials, so nothing here changes existing behavior
	// for that catalog.
	req.Header.Set(AccessDelegationHeader, AccessDelegationVendedCredentials)
	c.endpoint.setAuth(req)

	resp, err := c.endpoint.httpClient.Do(req)
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
		Name:               id.Table,
		BasePath:           basePath,
		DataPath:           dataPath,
		Format:             model.TableFormatIceberg,
		Properties:         properties,
		StorageCredentials: parseStorageCredentials(basePath, loaded.StorageCredentials, loaded.Config),
	}, nil
}

// ListTables walks a REST catalog's namespaces and tables. When database is set, only that
// namespace is walked; when it is empty, GET /v1/{prefix}/namespaces is paged first and every
// namespace it names is walked in turn. Each page follows next-page-token until the catalog stops
// returning one, so a paginated catalog is never truncated to its first page.
//
// filter.RequireConversionMarkers cannot be evaluated from the listing responses alone: neither
// GET .../namespaces nor GET .../namespaces/{namespace}/tables returns table properties, only
// identifiers. When that marker is required, this loads each candidate with GetSourceTable to read
// its properties before applying the filter; the common case (TableFilter{}, matching everything)
// takes no extra request.
//
// A failing page yields a zero identifier with a non-nil error and stops there, matching
// GlueConversionSource.ListTables' contract: a caller that only checks identifiers cannot mistake a
// truncated listing for a complete one, and a caller that stops iterating early (yield returning
// false) stops this paging with it rather than continuing to fetch pages nobody will see.
func (c *IcebergRESTConversionSource) ListTables(ctx context.Context, database string, filter TableFilter) iter.Seq2[TableIdentifier, error] {
	return func(yield func(TableIdentifier, error) bool) {
		if err := c.endpoint.negotiatePrefix(ctx); err != nil {
			yield(TableIdentifier{}, fmt.Errorf("failed to negotiate Iceberg REST catalog config: %w", err))
			return
		}

		database = strings.TrimSpace(database)

		var namespaces []string
		if database != "" {
			namespaces = []string{database}
		} else {
			var err error
			namespaces, err = c.listNamespaces(ctx)
			if err != nil {
				yield(TableIdentifier{}, err)
				return
			}
		}

		for _, ns := range namespaces {
			stopped, err := c.listTablesInNamespace(ctx, ns, filter, yield)
			if err != nil {
				yield(TableIdentifier{}, err)
				return
			}
			if stopped {
				return
			}
		}
	}
}

// listNamespaces pages through GET /v1/{prefix}/namespaces, returning every namespace as a single
// path segment (multi-level namespaces already joined with the unit separator the specification
// uses to address them).
func (c *IcebergRESTConversionSource) listNamespaces(ctx context.Context) ([]string, error) {
	var namespaces []string
	pageToken := ""
	for {
		endpoint := c.endpoint.path("namespaces")
		if pageToken != "" {
			endpoint += "?pageToken=" + url.QueryEscape(pageToken)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to build list-namespaces request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		c.endpoint.setAuth(req)

		resp, err := c.endpoint.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to list namespaces in Iceberg REST catalog: %w", err)
		}

		var parsed listNamespacesResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&parsed)
		status := resp.StatusCode
		_ = resp.Body.Close()

		if status >= 400 {
			return nil, fmt.Errorf("iceberg REST catalog returned %d listing namespaces", status)
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode list-namespaces response: %w", decodeErr)
		}

		for _, levels := range parsed.Namespaces {
			namespaces = append(namespaces, strings.Join(levels, nestedNamespaceSeparator))
		}

		if parsed.NextPageToken == "" {
			return namespaces, nil
		}
		pageToken = parsed.NextPageToken
	}
}

// listTablesInNamespace pages through GET /v1/{prefix}/namespaces/{ns}/tables, yielding every
// table that passes filter. It returns (true, nil) when yield asked the sequence to stop, so the
// caller's namespace loop stops with it instead of moving on to the next namespace.
func (c *IcebergRESTConversionSource) listTablesInNamespace(ctx context.Context, ns string, filter TableFilter, yield func(TableIdentifier, error) bool) (bool, error) {
	pageToken := ""
	for {
		endpoint := c.endpoint.path("namespaces", ns, "tables")
		if pageToken != "" {
			endpoint += "?pageToken=" + url.QueryEscape(pageToken)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return false, fmt.Errorf("failed to build list-tables request for namespace %q: %w", ns, err)
		}
		req.Header.Set("Accept", "application/json")
		c.endpoint.setAuth(req)

		resp, err := c.endpoint.httpClient.Do(req)
		if err != nil {
			return false, fmt.Errorf("failed to list tables in namespace %q: %w", ns, err)
		}

		var parsed listTablesResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&parsed)
		status := resp.StatusCode
		_ = resp.Body.Close()

		if status >= 400 {
			return false, fmt.Errorf("iceberg REST catalog returned %d listing tables in namespace %q", status, ns)
		}
		if decodeErr != nil {
			return false, fmt.Errorf("failed to decode list-tables response for namespace %q: %w", ns, decodeErr)
		}

		for _, ident := range parsed.Identifiers {
			name := strings.TrimSpace(ident.Name)
			if name == "" {
				continue
			}
			id := TableIdentifier{Database: ns, Table: name}

			if filter.RequireConversionMarkers {
				table, err := c.GetSourceTable(ctx, id)
				if err != nil {
					return false, fmt.Errorf("failed to load table %s while applying the listing filter: %w", id, err)
				}
				if !filter.Matches(table.Properties) {
					continue
				}
			}

			if !yield(id, nil) {
				return true, nil
			}
		}

		if parsed.NextPageToken == "" {
			return false, nil
		}
		pageToken = parsed.NextPageToken
	}
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
	c.endpoint.httpClient.CloseIdleConnections()
	return nil
}
