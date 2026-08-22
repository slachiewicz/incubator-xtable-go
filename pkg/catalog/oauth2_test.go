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

// These tests exercise oauth2Transport and the auth=oauth2 dispatch in restHTTPClient against a
// local httptest server standing in for an Iceberg REST catalog's /v1/oauth/tokens endpoint. None
// of this reaches, or claims to reach, a live Apache Polaris deployment or Snowflake Open Catalog:
// that leg is unverified.
package catalog_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
)

// tokenServer is a minimal /v1/oauth/tokens double. Each request must carry a well-formed
// client-credentials body; the response returned for it comes from a caller-supplied function
// (so the refresh tests can vary expires_in), and hits counts every request the handler receives.
type tokenServer struct {
	*httptest.Server

	mu       sync.Mutex
	form     url.Values  // last received form
	headers  http.Header // last received request headers
	hits     int32
	response func() (status int, body string)
}

func newTokenServer(t *testing.T, response func() (int, string)) *tokenServer {
	t.Helper()
	ts := &tokenServer{response: response}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&ts.hits, 1)

		require.NoError(t, r.ParseForm())
		ts.mu.Lock()
		ts.form = r.PostForm
		ts.headers = r.Header.Clone()
		ts.mu.Unlock()

		status, body := ts.response()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts
}

func (ts *tokenServer) hitCount() int {
	return int(atomic.LoadInt32(&ts.hits))
}

func (ts *tokenServer) lastForm() url.Values {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.form
}

func (ts *tokenServer) lastHeaders() http.Header {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.headers
}

func jsonToken(accessToken string, expiresIn int) string {
	body, _ := json.Marshal(map[string]any{
		"access_token": accessToken,
		"token_type":   "bearer",
		"expires_in":   expiresIn,
	})
	return string(body)
}

func TestOAuth2TransportAttachesBearerToken(t *testing.T) {
	t.Parallel()

	tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok-1", 3600) })

	var gotAuth string
	catalogSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer catalogSrv.Close()

	client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "client-a", "secret-a", "PRINCIPAL_ROLE:ALL", nil)

	resp, err := client.Get(catalogSrv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, "Bearer tok-1", gotAuth)
}

func TestOAuth2TransportSendsFormEncodedClientCredentialsBody(t *testing.T) {
	t.Parallel()

	tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok-1", 3600) })

	catalogSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer catalogSrv.Close()

	client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "client-a", "secret-a", "PRINCIPAL_ROLE:ALL", nil)

	resp, err := client.Get(catalogSrv.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()

	form := tokenSrv.lastForm()
	assert.Equal(t, "client_credentials", form.Get("grant_type"))
	assert.Equal(t, "client-a", form.Get("client_id"))
	assert.Equal(t, "secret-a", form.Get("client_secret"))
	assert.Equal(t, "PRINCIPAL_ROLE:ALL", form.Get("scope"))
}

func TestOAuth2TransportRefreshesShortLivedTokenButCachesLongLivedOne(t *testing.T) {
	t.Parallel()

	// Each subtest gets its own catalog server rather than sharing one from the parent: the
	// parent's own body returns as soon as its t.Parallel() subtests are queued (that is what
	// t.Parallel() does), so a plain "defer catalogSrv.Close()" here would close the server
	// before any subtest actually ran.
	newCatalogServer := func(t *testing.T) *httptest.Server {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)
		return srv
	}

	t.Run("short expiry re-fetches every request", func(t *testing.T) {
		t.Parallel()

		catalogSrv := newCatalogServer(t)
		// expires_in (1s) is well inside oauth2RefreshMargin (30s), so every request must treat
		// the cached token as already stale and fetch a fresh one -- no real sleep required.
		tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok", 1) })
		client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "id", "secret", "", nil)

		for i := 0; i < 3; i++ {
			resp, err := client.Get(catalogSrv.URL)
			require.NoError(t, err)
			_ = resp.Body.Close()
		}

		assert.Equal(t, 3, tokenSrv.hitCount(), "a token with expires_in inside the refresh margin must be re-fetched on every request")
	})

	t.Run("long expiry is cached", func(t *testing.T) {
		t.Parallel()

		catalogSrv := newCatalogServer(t)
		tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok", 3600) })
		client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "id", "secret", "", nil)

		for i := 0; i < 3; i++ {
			resp, err := client.Get(catalogSrv.URL)
			require.NoError(t, err)
			_ = resp.Body.Close()
		}

		assert.Equal(t, 1, tokenSrv.hitCount(), "a token comfortably outside the refresh margin must be fetched only once")
	})
}

func TestOAuth2TransportDoesNotMutateOriginalRequest(t *testing.T) {
	t.Parallel()

	tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok-1", 3600) })

	catalogSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer catalogSrv.Close()

	client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "id", "secret", "", nil)

	req, err := http.NewRequest(http.MethodGet, catalogSrv.URL, nil)
	require.NoError(t, err)
	require.Empty(t, req.Header.Get("Authorization"))

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Empty(t, req.Header.Get("Authorization"), "the original request must not be mutated by RoundTrip")
}

func TestOAuth2TransportSurfacesTokenEndpointFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp func() (int, string)
	}{
		{name: "401", resp: func() (int, string) { return http.StatusUnauthorized, `{"error":"invalid_client"}` }},
		{name: "malformed JSON", resp: func() (int, string) { return http.StatusOK, `not json` }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tokenSrv := newTokenServer(t, tt.resp)
			client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "id", "bad-secret", "", nil)

			_, err := client.Get("http://unused.invalid/should-not-be-reached")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "oauth2", "the error must name what failed rather than surfacing an unrelated network error")
			assert.Contains(t, err.Error(), tokenSrv.URL)
		})
	}
}

func TestOAuth2TransportConcurrentRequestsFetchTokenOnce(t *testing.T) {
	t.Parallel()

	tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok-1", 3600) })

	catalogSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer catalogSrv.Close()

	client := catalog.NewOAuth2HTTPClientWithSecret(5*time.Second, tokenSrv.URL, "id", "secret", "", nil)

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			resp, err := client.Get(catalogSrv.URL)
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
	assert.Equal(t, 1, tokenSrv.hitCount(), "20 concurrent requests through one client must fetch the token exactly once")
}

func TestOAuth2TransportAttachesExtraHeaders(t *testing.T) {
	t.Parallel()

	tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok-1", 3600) })

	var gotRealm string
	catalogSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRealm = r.Header.Get("Polaris-Realm")
		w.WriteHeader(http.StatusOK)
	}))
	defer catalogSrv.Close()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          catalogSrv.URL,
		Properties: map[string]string{
			catalog.PropCatalogAuth:                           "oauth2",
			catalog.PropCatalogOAuth2ClientID:                 "id",
			catalog.PropCatalogOAuth2ClientSecretEnv:          mustSetEnvSecret(t, "secret-value"),
			catalog.PropCatalogOAuth2TokenEndpoint:            tokenSrv.URL,
			catalog.PropCatalogHeaderPrefix + "Polaris-Realm": "POLARIS",
		},
	}

	client, token, err := catalog.RestHTTPClient(cfg, 5*time.Second)
	require.NoError(t, err)
	assert.Empty(t, token)

	resp, err := client.Get(catalogSrv.URL)
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, "POLARIS", gotRealm, "the catalog request must carry the extra header")
	assert.Equal(t, "POLARIS", tokenSrv.lastHeaders().Get("Polaris-Realm"), "the token exchange itself must carry the extra header too, since it lives on the catalog's own host")
}

// envSecretCounter gives each mustSetEnvSecret call a distinct variable name deterministically,
// rather than a wall-clock timestamp that parallel subtests could in principle collide on.
var envSecretCounter int64

// mustSetEnvSecret sets a uniquely named environment variable to value, registers its cleanup, and
// returns the variable's name -- so tests never share a mutable global environment variable name
// with each other even though every test in this file runs under t.Parallel().
func mustSetEnvSecret(t *testing.T, value string) string {
	t.Helper()
	name := fmt.Sprintf("POLYTABLE_TEST_OAUTH2_SECRET_%d", atomic.AddInt64(&envSecretCounter, 1))
	require.NoError(t, os.Setenv(name, value))
	t.Cleanup(func() { _ = os.Unsetenv(name) })
	return name
}

func TestRESTHTTPClientOAuth2AuthSpellings(t *testing.T) {
	t.Parallel()

	tokenSrv := newTokenServer(t, func() (int, string) { return http.StatusOK, jsonToken("tok-1", 3600) })

	tests := []struct {
		name string
		auth string
	}{
		{name: "oauth2", auth: "oauth2"},
		{name: "oauth", auth: "oauth"},
		{name: "client-credentials", auth: "client-credentials"},
		{name: "mixed case", auth: "OAuth2"},
		{name: "padded", auth: "  oauth2  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &catalog.Config{
				Type:         catalog.CatalogTypeIcebergREST,
				DatabaseName: "db",
				Properties: map[string]string{
					catalog.PropCatalogAuth:                  tt.auth,
					catalog.PropCatalogOAuth2ClientID:        "id",
					catalog.PropCatalogOAuth2ClientSecretEnv: mustSetEnvSecret(t, "secret-value"),
					catalog.PropCatalogOAuth2TokenEndpoint:   tokenSrv.URL,
				},
			}

			client, token, err := catalog.RestHTTPClient(cfg, time.Second)
			require.NoError(t, err)
			require.NotNil(t, client)
			assert.Empty(t, token, "an oauth2-authenticated client must not also carry a static token")
			assert.NotNil(t, client.Transport, "the oauth2 path must install an OAuth2 transport")
		})
	}
}

func TestRESTHTTPClientOAuth2DefaultsTokenEndpointFromURI(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          "https://example.com/polaris/api/catalog",
		Properties: map[string]string{
			catalog.PropCatalogAuth:                  "oauth2",
			catalog.PropCatalogOAuth2ClientID:        "id",
			catalog.PropCatalogOAuth2ClientSecretEnv: mustSetEnvSecret(t, "secret-value"),
		},
	}

	client, token, err := catalog.RestHTTPClient(cfg, time.Second)
	require.NoError(t, err, "the token endpoint must default to <uri>/v1/oauth/tokens")
	require.NotNil(t, client)
	assert.Empty(t, token)
}

func TestRESTHTTPClientOAuth2RequiresClientID(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          "https://example.com/catalog",
		Properties: map[string]string{
			catalog.PropCatalogAuth:                  "oauth2",
			catalog.PropCatalogOAuth2ClientSecretEnv: mustSetEnvSecret(t, "secret-value"),
		},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), catalog.PropCatalogOAuth2ClientID)
}

func TestRESTHTTPClientOAuth2RequiresClientSecretEnvProperty(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          "https://example.com/catalog",
		Properties: map[string]string{
			catalog.PropCatalogAuth:           "oauth2",
			catalog.PropCatalogOAuth2ClientID: "id",
		},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), catalog.PropCatalogOAuth2ClientSecretEnv)
}

func TestRESTHTTPClientOAuth2ErrorsWhenSecretEnvVarUnset(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		URI:          "https://example.com/catalog",
		Properties: map[string]string{
			catalog.PropCatalogAuth:                  "oauth2",
			catalog.PropCatalogOAuth2ClientID:        "id",
			catalog.PropCatalogOAuth2ClientSecretEnv: "POLYTABLE_TEST_OAUTH2_SECRET_NEVER_SET",
		},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POLYTABLE_TEST_OAUTH2_SECRET_NEVER_SET", "the error must name the empty variable, not just the property")
}

func TestRESTHTTPClientUnknownAuthErrorMentionsOAuth2Spellings(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		Properties:   map[string]string{catalog.PropCatalogAuth: "bogus"},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus")
	assert.Contains(t, err.Error(), "oauth2")
}
