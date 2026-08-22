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
	"sync"
)

// restConfigResponse is the subset of the Iceberg REST GET /v1/config response this package reads.
// The response also carries "defaults", which no call site needs yet.
type restConfigResponse struct {
	Overrides struct {
		Prefix string `json:"prefix"`
	} `json:"overrides"`
	// Endpoints, when present, is the exhaustive list of routes this catalog serves, each formatted
	// as "<METHOD> <path>" (e.g. "GET /v1/{prefix}/namespaces"). It is documented as optional: a nil
	// slice means the catalog did not say, not that it serves nothing.
	Endpoints []string `json:"endpoints"`
}

// restCatalogEndpoint centralizes what IcebergRESTCatalogClient (the write side) and
// IcebergRESTConversionSource (the read side) both need to address an Iceberg REST catalog: the
// HTTP client and bearer token, the base URI, and the GET /v1/config prefix negotiation the
// specification requires before either side can build a single working path. Before T53 both files
// hardcoded a prefix-less /v1/namespaces/... form, which happened to work only because the
// catalogs exercised so far (Nessie, tabulario/iceberg-rest) return an empty prefix.
type restCatalogEndpoint struct {
	httpClient *http.Client
	baseURI    string
	authToken  string
	warehouse  string

	prefixMu   sync.Mutex
	negotiated bool
	prefix     string
	// endpoints mirrors restConfigResponse.Endpoints once negotiation has run; nil until then, and
	// still nil afterward if the catalog's response carried no such field.
	endpoints []string
}

// newRESTCatalogEndpoint builds a restCatalogEndpoint. properties may be nil; only
// PropCatalogWarehouse is read from it here, everything else (auth mode, token, scope) is already
// resolved into httpClient and authToken by restHTTPClient before this is called.
func newRESTCatalogEndpoint(httpClient *http.Client, baseURI, authToken string, properties map[string]string) *restCatalogEndpoint {
	return &restCatalogEndpoint{
		httpClient: httpClient,
		baseURI:    strings.TrimSuffix(baseURI, "/"),
		authToken:  authToken,
		warehouse:  strings.TrimSpace(properties[PropCatalogWarehouse]),
	}
}

// negotiatePrefix resolves the catalog's path prefix by calling GET /v1/config, at most once for
// the lifetime of this endpoint -- but only a *terminal* outcome is cached. A call in the
// constructor is not an option (a constructor has no context to make it with), and a catalog that
// has no /v1/config route at all must not fail construction, only fall back to the historical
// prefix-less paths; a 404 or 405 is read as exactly that ("this catalog predates the endpoint")
// and latched as success with an empty prefix, same as a decoded 200. A transport error or an
// unexpected status is different and is deliberately *not* latched: it means negotiation did not
// resolve anything one way or the other, so it is surfaced to the operation that triggered it and
// the next operation gets to try again rather than being permanently bricked by one transient
// blip (a canceled context, a dropped connection) for the rest of the process's life.
//
// The mutex is held across the fetch itself, not just around the "have we resolved this" check, so
// concurrent first callers single-flight into one actual GET /v1/config rather than each starting
// their own.
func (e *restCatalogEndpoint) negotiatePrefix(ctx context.Context) error {
	e.prefixMu.Lock()
	defer e.prefixMu.Unlock()

	if e.negotiated {
		return nil
	}

	prefix, endpoints, err := fetchRESTConfig(ctx, e.httpClient, e.baseURI, e.authToken, e.warehouse)
	if err != nil {
		return err
	}

	e.prefix, e.endpoints, e.negotiated = prefix, endpoints, true
	return nil
}

// fetchRESTConfig calls GET {baseURI}/v1/config, appending ?warehouse=<warehouse> only when one is
// configured, and returns the negotiated prefix and the advertised endpoints (if any).
func fetchRESTConfig(ctx context.Context, httpClient *http.Client, baseURI, authToken, warehouse string) (prefix string, endpoints []string, err error) {
	endpointURL := baseURI + "/v1/config"
	if warehouse != "" {
		endpointURL += "?warehouse=" + url.QueryEscape(warehouse)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to build Iceberg REST catalog config request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("failed to reach Iceberg REST catalog config endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		// This catalog predates GET /v1/config: fall back to today's prefix-less behavior.
		return "", nil, nil
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", nil, fmt.Errorf("iceberg REST catalog config endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed restConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", nil, fmt.Errorf("failed to decode Iceberg REST catalog config response: %w", err)
	}
	return parsed.Overrides.Prefix, parsed.Endpoints, nil
}

// path builds a /v1/... URL under this endpoint's negotiated prefix, escaping every segment with
// url.PathEscape -- the way GetSourceTable already did before T53, now applied uniformly to every
// call site. A prefix that itself contains a "/" (OneLake's is "<workspace>/<item>") is split and
// each part escaped individually rather than escaped as one opaque segment, so the "/" it contains
// stays a path separator instead of becoming a literal "%2F".
func (e *restCatalogEndpoint) path(segments ...string) string {
	var b strings.Builder
	b.WriteString(e.baseURI)
	b.WriteString("/v1")
	if e.prefix != "" {
		for _, part := range strings.Split(e.prefix, "/") {
			b.WriteByte('/')
			b.WriteString(url.PathEscape(part))
		}
	}
	for _, seg := range segments {
		b.WriteByte('/')
		b.WriteString(url.PathEscape(seg))
	}
	return b.String()
}

// setAuth attaches the bearer token to req the way every call site here already did before T53:
// only when a static token is configured. An Entra-authenticated client (restHTTPClient's "entra"
// path) carries no static token at all; its transport attaches a refreshed one itself.
func (e *restCatalogEndpoint) setAuth(req *http.Request) {
	if e.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.authToken)
	}
}

// writeEndpointAdvertised reports whether the catalog's /v1/config response, if it carried an
// endpoints array at all, named at least one write route over namespaces or tables (POST, PUT or
// DELETE against a path containing "/tables" or "/namespaces"). known is false when endpoints is
// nil, meaning the catalog did not say -- either /v1/config predates the field, or negotiation has
// not run -- and that must not be read as "read-only": a catalog that never mentions its endpoint
// list is not thereby assumed to serve none of them.
//
// The path check matters because a real catalog's endpoints array routinely names writes that are
// not table writes at all -- "POST /v1/oauth/tokens" is the common one -- and counting any POST as
// proof of a writable catalog would let that mask a table endpoint that is genuinely read-only.
func (e *restCatalogEndpoint) writeEndpointAdvertised() (known, advertised bool) {
	if e.endpoints == nil {
		return false, false
	}
	for _, ep := range e.endpoints {
		method, path, ok := strings.Cut(strings.TrimSpace(ep), " ")
		if !ok {
			continue
		}
		if !strings.Contains(path, "/tables") && !strings.Contains(path, "/namespaces") {
			continue
		}
		switch strings.ToUpper(method) {
		case http.MethodPost, http.MethodPut, http.MethodDelete:
			return true, true
		}
	}
	return true, false
}

// readOnlyCatalogError reports that operation cannot succeed against this endpoint because the
// catalog is read-only, naming both the endpoint and the refused operation rather than surfacing a
// bare HTTP status.
func readOnlyCatalogError(baseURI, operation string) error {
	return fmt.Errorf("iceberg REST catalog at %s is read-only: %s is not supported", baseURI, operation)
}
