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

package catalog

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

// entraTransport is an http.RoundTripper that attaches a Microsoft Entra ID bearer token to every
// request, refreshing it from cred shortly before it expires. A REST catalog client keeps one of
// these for the lifetime of a (possibly long-running) sync, so the token must never be allowed to
// go stale mid-request.
type entraTransport struct {
	base   http.RoundTripper
	cred   azcore.TokenCredential
	scopes []string

	mu    sync.Mutex
	token azcore.AccessToken
}

// RoundTrip implements http.RoundTripper. It never mutates req, as required by the RoundTripper
// contract: it clones the request and sets the Authorization header on the clone.
func (t *entraTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.currentToken(req)
	if err != nil {
		return nil, err
	}

	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+token)

	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

// currentToken returns a cached token if it is still valid for at least 5 minutes, refreshing it
// under the mutex otherwise. The lock is held across the check and, when needed, the refresh call
// to cred.GetToken, so concurrent callers do not race to refresh the same token; it is never held
// across the subsequent base.RoundTrip call, which happens back in RoundTrip after this returns.
func (t *entraTransport) currentToken(req *http.Request) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.token.Token == "" || time.Until(t.token.ExpiresOn) < 5*time.Minute {
		tok, err := t.cred.GetToken(req.Context(), policy.TokenRequestOptions{Scopes: t.scopes})
		if err != nil {
			return "", fmt.Errorf("failed to acquire an Entra ID token for %v: %w", t.scopes, err)
		}
		t.token = tok
	}
	return t.token.Token, nil
}

// newEntraHTTPClient builds an HTTP client whose every request carries a current Entra ID token.
// It calls azidentity.NewDefaultAzureCredential, which alone covers workload identity, managed
// identity, an environment service principal and the Azure CLI, so no per-mechanism configuration
// is needed here.
func newEntraHTTPClient(timeout time.Duration, scopes []string) (*http.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create an Entra ID default credential: %w", err)
	}
	return NewEntraHTTPClientWithCredential(cred, timeout, scopes), nil
}

// NewEntraHTTPClientWithCredential builds the same client over a caller-supplied credential, which
// is how the tests drive it without an Azure tenant.
func NewEntraHTTPClientWithCredential(cred azcore.TokenCredential, timeout time.Duration, scopes []string) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &entraTransport{
			cred:   cred,
			scopes: scopes,
		},
	}
}
