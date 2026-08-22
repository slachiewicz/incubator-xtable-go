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

// Package catalog_test covers docs/improvement-plan.md T64: catalogs (Iceberg REST) that vend
// short-lived storage credentials scoped to a table, in place of a customer holding bucket-wide
// credentials. The fixtures model the response Snowflake's Iceberg REST catalog returned against a
// live account on 2026-08-22 (see the "config" shape below); the "storage-credentials" array shape
// is the current Iceberg REST specification's documented alternative and was not exercised against
// a real server.
package catalog_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
)

// newVendingRESTServer builds an httptest server answering GET /v1/config (so prefix negotiation
// falls back to an empty prefix, matching a catalog that predates negotiation) and one load-table
// route serving body for the given namespace/table. It also records every request path and header
// set it saw, for assertions on the delegation header.
func newVendingRESTServer(t *testing.T, namespace, table, body string) (*httptest.Server, *[]http.Header) {
	t.Helper()
	var headers []http.Header

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc(fmt.Sprintf("/v1/namespaces/%s/tables/%s", namespace, table), func(w http.ResponseWriter, r *http.Request) {
		headers = append(headers, r.Header.Clone())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, &headers
}

// TestGetSourceTable_SendsDelegationHeader proves the request that resolves a table always asks for
// vended credentials, whether or not the catalog answering it supports the mechanism -- the ask
// itself must never regress a catalog that ignores it.
func TestGetSourceTable_SendsDelegationHeader(t *testing.T) {
	t.Parallel()

	body := `{"metadata-location":"s3://bucket/db/tbl/metadata/v1.json","metadata":{"location":"s3://bucket/db/tbl"}}`
	server, headers := newVendingRESTServer(t, "db", "tbl", body)

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "db", "")
	_, err := src.GetSourceTable(t.Context(), catalog.TableIdentifier{Database: "db", Table: "tbl"})
	require.NoError(t, err)

	require.Len(t, *headers, 1)
	assert.Equal(t, catalog.AccessDelegationVendedCredentials, (*headers)[0].Get(catalog.AccessDelegationHeader))
}

// TestGetSourceTable_StorageCredentials covers both response shapes this code parses, a catalog
// that vends nothing at all (which must leave SourceTable.StorageCredentials nil rather than a
// zero-valued struct, so callers can tell "no credentials" from "empty credentials"), and the region
// being carried through -- the fix for the PermanentRedirect a vended credential without its region
// reproduces.
func TestGetSourceTable_StorageCredentials(t *testing.T) {
	t.Parallel()

	basePath := "s3://sfc-prod3-bucket/iceberg/db/tbl"
	loadBody := func(t *testing.T, extra string) string {
		t.Helper()
		return fmt.Sprintf(`{
			"metadata-location":"%s/metadata/v1.json",
			"metadata":{"location":"%s"},
			%s
		}`, basePath, basePath, extra)
	}

	tests := []struct {
		name          string
		extra         string
		wantNil       bool
		wantAccessKey string
		wantSecret    string
		wantToken     string
		wantRegion    string
	}{
		{
			// The exact shape a live Snowflake-managed table returns, and the reason the two blocks
			// must be merged rather than chosen between: the storage-credentials entry carries the
			// key, secret, token and expiry but NOT client.region, which appears only in the
			// top-level config. Taking the array alone yields a credential with no region, and the
			// PermanentRedirect that region exists to prevent -- which is exactly what happened
			// against Snowflake before this was fixed.
			name: "region comes from the top-level config when storage-credentials omits it",
			extra: `"config":{"client.region":"us-west-2"},
			"storage-credentials":[{
				"prefix":"` + basePath + `",
				"config":{
					"s3.access-key-id":"AKIASPLITSHAPE",
					"s3.secret-access-key":"split-secret",
					"s3.session-token":"split-token",
					"expiration-time":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"
				}
			}]`,
			wantAccessKey: "AKIASPLITSHAPE",
			wantSecret:    "split-secret",
			wantToken:     "split-token",
			wantRegion:    "us-west-2",
		},
		{
			// Exactly the shape observed against a live Snowflake-managed Iceberg table.
			name: "flat config shape observed against Snowflake",
			extra: `"config":{
				"s3.access-key-id":"AKIAFLATSHAPE",
				"s3.secret-access-key":"flat-secret",
				"s3.session-token":"flat-token",
				"expiration-time":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `",
				"client.region":"us-west-2"
			}`,
			wantAccessKey: "AKIAFLATSHAPE",
			wantSecret:    "flat-secret",
			wantToken:     "flat-token",
			wantRegion:    "us-west-2",
		},
		{
			// The current specification's array shape, prefix-scoped to the table's own base path.
			name: "storage-credentials array shape, spec-documented",
			extra: `"storage-credentials":[{
				"prefix":"` + basePath + `",
				"config":{
					"s3.access-key-id":"AKIAARRAYSHAPE",
					"s3.secret-access-key":"array-secret",
					"s3.session-token":"array-token",
					"expiration-time":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `",
					"client.region":"eu-central-1"
				}
			}]`,
			wantAccessKey: "AKIAARRAYSHAPE",
			wantSecret:    "array-secret",
			wantToken:     "array-token",
			wantRegion:    "eu-central-1",
		},
		{
			// Both shapes present: storage-credentials must win.
			name: "storage-credentials preferred over flat config when both present",
			extra: `"config":{
				"s3.access-key-id":"AKIAFLATLOSES",
				"s3.secret-access-key":"flat-secret-loses",
				"client.region":"us-east-1"
			},
			"storage-credentials":[{
				"prefix":"` + basePath + `",
				"config":{
					"s3.access-key-id":"AKIAARRAYWINS",
					"s3.secret-access-key":"array-secret-wins",
					"client.region":"ap-southeast-2"
				}
			}]`,
			wantAccessKey: "AKIAARRAYWINS",
			wantSecret:    "array-secret-wins",
			wantRegion:    "ap-southeast-2",
		},
		{
			// A storage-credentials entry whose prefix does not match the table's base path is
			// skipped; nothing else matches either, so this is the "vends nothing usable" case.
			name: "storage-credentials entry with a non-matching prefix is ignored",
			extra: `"storage-credentials":[{
				"prefix":"s3://a-different-bucket/",
				"config":{
					"s3.access-key-id":"AKIAWRONGPREFIX",
					"s3.secret-access-key":"wrong-prefix-secret"
				}
			}]`,
			wantNil: true,
		},
		{
			// The regression case that matters most: a catalog (e.g. Glue-backed or a plain
			// tabulario/iceberg-rest instance) that never vends anything at all. Behavior must be
			// unchanged: StorageCredentials is nil, not a zero-valued struct.
			name:    "no credentials vended at all",
			extra:   `"metadata":{"location":"` + basePath + `"}`,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server, _ := newVendingRESTServer(t, "db", "tbl", loadBody(t, tt.extra))
			src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "db", "")

			resolved, err := src.GetSourceTable(t.Context(), catalog.TableIdentifier{Database: "db", Table: "tbl"})
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, resolved.StorageCredentials)
				return
			}

			require.NotNil(t, resolved.StorageCredentials)
			assert.Equal(t, tt.wantAccessKey, resolved.StorageCredentials.AccessKeyID)
			assert.Equal(t, tt.wantSecret, resolved.StorageCredentials.SecretAccessKey)
			if tt.wantToken != "" {
				assert.Equal(t, tt.wantToken, resolved.StorageCredentials.SessionToken)
			}
			assert.Equal(t, tt.wantRegion, resolved.StorageCredentials.Region)
			if strings.Contains(tt.extra, "expiration-time") {
				assert.False(t, resolved.StorageCredentials.Expiration.IsZero(),
					"expiration-time was supplied and should have parsed")
			}
		})
	}
}

// TestStorageCredentials_Expired covers the expiry check itself, independent of parsing: an
// already-expired credential, one inside the buffer window, one safely before it, and one carrying
// no expiration at all (which must never be treated as expired).
func TestStorageCredentials_Expired(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		expiration time.Time
		buffer     time.Duration
		want       bool
	}{
		{name: "already expired", expiration: now.Add(-time.Minute), buffer: 0, want: true},
		{name: "expires exactly now", expiration: now, buffer: 0, want: true},
		{name: "safely in the future, no buffer", expiration: now.Add(time.Hour), buffer: 0, want: false},
		{name: "within the near-expiry buffer", expiration: now.Add(30 * time.Second), buffer: time.Minute, want: true},
		{name: "outside the near-expiry buffer", expiration: now.Add(5 * time.Minute), buffer: time.Minute, want: false},
		{name: "no expiration reported at all", expiration: time.Time{}, buffer: time.Hour, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			creds := catalog.StorageCredentials{Expiration: tt.expiration}
			assert.Equal(t, tt.want, creds.Expired(now, tt.buffer))
		})
	}
}

// TestStorageCredentials_Redacted proves a StorageCredentials value cannot leak its secret through
// any of the common ways a value ends up in a log line or an error message: %v, %s, %+v, %#v and
// encoding/json.
func TestStorageCredentials_Redacted(t *testing.T) {
	t.Parallel()

	creds := catalog.StorageCredentials{
		AccessKeyID:     "AKIASECRETID",
		SecretAccessKey: "super-secret-key-value",
		SessionToken:    "super-secret-session-token",
		Region:          "us-east-1",
	}
	secrets := []string{creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken}

	renderings := map[string]string{
		"%v":  fmt.Sprintf("%v", creds),
		"%s":  creds.String(),
		"%+v": fmt.Sprintf("%+v", creds),
		"%#v": fmt.Sprintf("%#v", creds),
	}
	jsonBytes, err := json.Marshal(creds)
	require.NoError(t, err)
	renderings["json.Marshal"] = string(jsonBytes)

	for verb, rendered := range renderings {
		for _, secret := range secrets {
			assert.NotContains(t, rendered, secret, "verb %s leaked a secret: %s", verb, rendered)
		}
	}

	// Also cover a SourceTable embedding the credentials, the realistic shape a caller would
	// actually have in hand when tempted to log it.
	table := catalog.SourceTable{Name: "tbl", StorageCredentials: &creds}
	renderedTable := fmt.Sprintf("%+v", table)
	for _, secret := range secrets {
		assert.NotContains(t, renderedTable, secret)
	}
}
