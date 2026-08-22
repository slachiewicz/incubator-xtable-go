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

package catalog_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
)

// fakeCredential is a minimal azcore.TokenCredential the tests drive without an Azure tenant. Each
// call to GetToken returns the next entry in tokens (repeating the last one once exhausted, unless
// err is set) and increments calls.
type fakeCredential struct {
	mu     sync.Mutex
	tokens []azcore.AccessToken
	err    error
	calls  int32
}

func (f *fakeCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	atomic.AddInt32(&f.calls, 1)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return azcore.AccessToken{}, f.err
	}
	if len(f.tokens) == 0 {
		return azcore.AccessToken{}, errors.New("fakeCredential: no tokens configured")
	}
	idx := int(atomic.LoadInt32(&f.calls)) - 1
	if idx >= len(f.tokens) {
		idx = len(f.tokens) - 1
	}
	return f.tokens[idx], nil
}

func (f *fakeCredential) callCount() int {
	return int(atomic.LoadInt32(&f.calls))
}

func TestEntraTransportAttachesBearerToken(t *testing.T) {
	t.Parallel()

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cred := &fakeCredential{tokens: []azcore.AccessToken{
		{Token: "tok-1", ExpiresOn: time.Now().Add(time.Hour)},
	}}
	client := catalog.NewEntraHTTPClientWithCredential(cred, 5*time.Second, []string{"scope-a"})

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, "Bearer tok-1", gotAuth)
}

func TestEntraTransportReusesValidToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cred := &fakeCredential{tokens: []azcore.AccessToken{
		{Token: "tok-1", ExpiresOn: time.Now().Add(time.Hour)},
	}}
	client := catalog.NewEntraHTTPClientWithCredential(cred, 5*time.Second, []string{"scope-a"})

	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}

	assert.Equal(t, 1, cred.callCount())
}

func TestEntraTransportRefreshesExpiringToken(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var seenAuth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seenAuth = append(seenAuth, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cred := &fakeCredential{tokens: []azcore.AccessToken{
		// Expires in under 5 minutes: every request must trigger a refresh.
		{Token: "tok-1", ExpiresOn: time.Now().Add(2 * time.Minute)},
		{Token: "tok-2", ExpiresOn: time.Now().Add(2 * time.Minute)},
	}}
	client := catalog.NewEntraHTTPClientWithCredential(cred, 5*time.Second, []string{"scope-a"})

	for i := 0; i < 2; i++ {
		resp, err := client.Get(srv.URL)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}

	assert.Equal(t, 2, cred.callCount())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seenAuth, 2)
	assert.Equal(t, "Bearer tok-1", seenAuth[0])
	assert.Equal(t, "Bearer tok-2", seenAuth[1])
}

func TestEntraTransportSurfacesTokenAcquisitionFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cred := &fakeCredential{err: errors.New("boom")}
	client := catalog.NewEntraHTTPClientWithCredential(cred, 5*time.Second, []string{"scope-a"})

	_, err := client.Get(srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scope-a")
	assert.Contains(t, err.Error(), "boom")
}

func TestEntraTransportDoesNotMutateOriginalRequest(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cred := &fakeCredential{tokens: []azcore.AccessToken{
		{Token: "tok-1", ExpiresOn: time.Now().Add(time.Hour)},
	}}
	client := catalog.NewEntraHTTPClientWithCredential(cred, 5*time.Second, []string{"scope-a"})

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	require.Empty(t, req.Header.Get("Authorization"))

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Empty(t, req.Header.Get("Authorization"), "the original request must not be mutated by RoundTrip")
}

func TestEntraTransportConcurrentRequests(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cred := &fakeCredential{tokens: []azcore.AccessToken{
		{Token: "tok-1", ExpiresOn: time.Now().Add(time.Hour)},
	}}
	client := catalog.NewEntraHTTPClientWithCredential(cred, 5*time.Second, []string{"scope-a"})

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

func TestRESTHTTPClientDefaultsToStaticToken(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		Properties:   map[string]string{catalog.PropCatalogToken: "static-tok"},
	}

	client, token, err := catalog.RestHTTPClient(cfg, time.Second)
	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Equal(t, "static-tok", token)
	assert.Nil(t, client.Transport, "the static-token path must not install an Entra transport")
}

func TestRESTHTTPClientEntraAuthSpellings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth string
	}{
		{name: "entra", auth: "entra"},
		{name: "entra-id", auth: "entra-id"},
		{name: "azure", auth: "azure"},
		{name: "mixed case", auth: "Entra-ID"},
		{name: "padded", auth: "  entra  "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &catalog.Config{
				Type:         catalog.CatalogTypeIcebergREST,
				DatabaseName: "db",
				Properties:   map[string]string{catalog.PropCatalogAuth: tt.auth},
			}

			client, token, err := catalog.RestHTTPClient(cfg, time.Second)
			require.NoError(t, err)
			require.NotNil(t, client)
			assert.Empty(t, token, "an Entra-authenticated client must not also carry a static token")
			assert.NotNil(t, client.Transport, "the entra path must install an Entra transport")
		})
	}
}

func TestRESTHTTPClientUnknownAuthErrors(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
		Properties:   map[string]string{catalog.PropCatalogAuth: "bogus"},
	}

	_, _, err := catalog.RestHTTPClient(cfg, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus")
}

func TestRESTHTTPClientNilPropertiesDoesNotPanic(t *testing.T) {
	t.Parallel()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "db",
	}

	require.NotPanics(t, func() {
		client, token, err := catalog.RestHTTPClient(cfg, time.Second)
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.Empty(t, token)
	})
}
