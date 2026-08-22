//go:build !js

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

// These tests exercise sigv4Transport against a local httptest server and static credentials.
// They verify signing is self-consistent -- the request carries the shape SigV4 requires and the
// credential scope names the configured region and service -- and that a body survives being
// hashed and resent intact. None of this reaches, or claims to reach, a live Glue or S3 Tables
// endpoint: that leg is unverified.
package catalog_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
)

// staticCreds builds a client over unrotating, in-memory credentials, so signing output is
// deterministic across a whole test run.
func staticCreds() credentials.StaticCredentialsProvider {
	return credentials.NewStaticCredentialsProvider("AKIAEXAMPLE", "secretexample", "")
}

func TestSigV4TransportSignsRequest(t *testing.T) {
	t.Parallel()

	var gotAuth, gotDate, gotContentSha string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotDate = r.Header.Get("X-Amz-Date")
		gotContentSha = r.Header.Get("X-Amz-Content-Sha256")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := catalog.NewSigV4HTTPClientWithCredentials(staticCreds(), 5*time.Second, "us-east-1", "glue")

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.True(t, strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 "), "Authorization header must start with the SigV4 algorithm: %q", gotAuth)
	assert.NotEmpty(t, gotDate, "X-Amz-Date must be set")
	assert.NotEmpty(t, gotContentSha, "X-Amz-Content-Sha256 must be set")
}

func TestSigV4TransportCredentialScopeNamesRegionAndService(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := catalog.NewSigV4HTTPClientWithCredentials(staticCreds(), 5*time.Second, "eu-west-2", "s3tables")

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// gotAuth looks like:
	//   AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260101/eu-west-2/s3tables/aws4_request, SignedHeaders=..., Signature=...
	credPart := extractCredentialField(t, gotAuth)
	scope := strings.Split(credPart, "/")
	require.Len(t, scope, 5, "credential scope must have 5 slash-separated fields: %q", credPart)
	assert.Equal(t, "eu-west-2", scope[2], "credential scope must name the configured region")
	assert.Equal(t, "s3tables", scope[3], "credential scope must name the configured signing name")
	assert.Equal(t, "aws4_request", scope[4])
}

func extractCredentialField(t *testing.T, authHeader string) string {
	t.Helper()
	require.True(t, strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 "), "unexpected Authorization header: %q", authHeader)
	const marker = "Credential="
	idx := strings.Index(authHeader, marker)
	require.GreaterOrEqual(t, idx, 0, "Authorization header must carry a Credential field: %q", authHeader)
	rest := authHeader[idx+len(marker):]
	end := strings.Index(rest, ",")
	require.GreaterOrEqual(t, end, 0, "Credential field must be comma-terminated: %q", authHeader)
	return rest[:end]
}

func TestSigV4TransportGetBodylessSignsEmptyPayloadHash(t *testing.T) {
	t.Parallel()

	const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	var gotContentSha string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentSha = r.Header.Get("X-Amz-Content-Sha256")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := catalog.NewSigV4HTTPClientWithCredentials(staticCreds(), 5*time.Second, "us-east-1", "glue")

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, emptyHash, gotContentSha)
}

func TestSigV4TransportPostSignsBodyHashAndForwardsBodyIntact(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"table":"orders","action":"commit"}`)
	sum := sha256.Sum256(payload)
	wantHash := hex.EncodeToString(sum[:])

	var gotContentSha string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentSha = r.Header.Get("X-Amz-Content-Sha256")
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := catalog.NewSigV4HTTPClientWithCredentials(staticCreds(), 5*time.Second, "us-east-1", "glue")

	resp, err := client.Post(srv.URL, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, wantHash, gotContentSha, "the signed payload hash must match the body's SHA-256")
	assert.Equal(t, payload, gotBody, "the server must receive the body intact after it was buffered for signing")
}

func TestSigV4TransportDoesNotMutateOriginalRequest(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := catalog.NewSigV4HTTPClientWithCredentials(staticCreds(), 5*time.Second, "us-east-1", "glue")

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	require.Empty(t, req.Header.Get("Authorization"))

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Empty(t, req.Header.Get("Authorization"), "the original request must not be mutated by RoundTrip")
}

func TestSigV4TransportConcurrentRequests(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := catalog.NewSigV4HTTPClientWithCredentials(staticCreds(), 5*time.Second, "us-east-1", "glue")

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			resp, err := client.Get(srv.URL)
			if err != nil {
				errCh <- err
				return
			}
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestRESTHTTPClientSigV4AuthSpellings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth string
	}{
		{name: "sigv4", auth: "sigv4"},
		{name: "aws", auth: "aws"},
		{name: "mixed case", auth: "SigV4"},
		{name: "padded", auth: "  aws  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &catalog.Config{
				Type:         catalog.CatalogTypeIcebergREST,
				DatabaseName: "db",
				URI:          "https://glue.us-east-1.amazonaws.com/iceberg",
				Properties:   map[string]string{catalog.PropCatalogAuth: tt.auth},
			}

			client, token, err := catalog.RestHTTPClient(cfg, time.Second)
			require.NoError(t, err)
			require.NotNil(t, client)
			assert.Empty(t, token, "a SigV4-authenticated client must not also carry a static token")
			assert.NotNil(t, client.Transport, "the sigv4 path must install a SigV4 transport")
		})
	}
}

func TestRESTHTTPClientSigV4DerivesRegionAndSigningNameFromURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  string
	}{
		{name: "glue", uri: "https://glue.us-west-2.amazonaws.com/iceberg"},
		{name: "s3tables", uri: "https://s3tables.eu-central-1.amazonaws.com/iceberg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &catalog.Config{
				Type:         catalog.CatalogTypeIcebergREST,
				DatabaseName: "db",
				URI:          tt.uri,
				Properties:   map[string]string{catalog.PropCatalogAuth: "sigv4"},
			}

			client, token, err := catalog.RestHTTPClient(cfg, time.Second)
			require.NoError(t, err, "region and signing name must be derivable from the URI host without explicit properties")
			require.NotNil(t, client)
			assert.Empty(t, token)
		})
	}
}

func TestRESTHTTPClientSigV4RequiresSigningNameWhenNotDerivable(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          "https://catalog.example.com/iceberg",
		Properties:   map[string]string{catalog.PropCatalogAuth: "sigv4"},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), catalog.PropCatalogSigningName)
}

func TestRESTHTTPClientSigV4ExplicitPropertiesOverrideDerivedOnes(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          "https://catalog.example.com/iceberg",
		Properties: map[string]string{
			catalog.PropCatalogAuth:        "aws",
			catalog.PropCatalogRegion:      "ap-southeast-1",
			catalog.PropCatalogSigningName: "glue",
		},
	}

	client, token, err := catalog.RestHTTPClient(cfg, time.Second)
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Empty(t, token)
}

func TestRESTHTTPClientUnknownAuthErrorsMentionsSigV4Spellings(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		Properties:   map[string]string{catalog.PropCatalogAuth: "bogus"},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus")
	assert.Contains(t, err.Error(), "sigv4")
}
