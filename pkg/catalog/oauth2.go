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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// oauth2RefreshMargin is how far ahead of a token's reported expiry this transport refetches it
// rather than presenting it. It plays the same role as entraTransport's 5-minute margin, but is
// much smaller: OAuth2 client-credentials tokens from an Iceberg REST catalog (Polaris and
// Snowflake Open Catalog observed so far) are commonly issued with a short expires_in -- an hour
// or less -- and a 5-minute margin against that would mean refreshing needlessly often. 30 seconds
// is enough slack for the token to still be accepted by the time it reaches the catalog, without
// discarding a large fraction of a short-lived token's useful life.
const oauth2RefreshMargin = 30 * time.Second

// oauth2Transport is an http.RoundTripper that attaches an OAuth2 client-credentials bearer token
// to every request, fetching and refreshing it from tokenURL as needed. This is the Iceberg REST
// specification's own authentication mechanism (POST /v1/oauth/tokens), and is what Apache Polaris
// and Snowflake Open Catalog (which is Polaris) speak natively.
type oauth2Transport struct {
	base         http.RoundTripper
	tokenClient  *http.Client
	tokenURL     string
	clientID     string
	clientSecret string
	scope        string

	mu     sync.Mutex
	token  string
	expiry time.Time
}

// RoundTrip implements http.RoundTripper. It never mutates req, as required by the RoundTripper
// contract: it clones the request and sets the Authorization header on the clone, the same
// discipline entraTransport and sigv4Transport follow.
func (t *oauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
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

// currentToken returns a cached token if it is still valid for at least oauth2RefreshMargin,
// fetching a new one under the mutex otherwise. As in entraTransport, the lock is held across the
// check and, when needed, the token-endpoint round trip, so concurrent callers do not race to fetch
// the same token; it is never held across the caller's own request, which happens back in
// RoundTrip after this returns.
func (t *oauth2Transport) currentToken(req *http.Request) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.token == "" || time.Until(t.expiry) < oauth2RefreshMargin {
		token, expiresIn, err := t.fetchToken(req)
		if err != nil {
			return "", err
		}
		t.token = token
		// expiresIn <= 0 means the token endpoint did not report an expiry (or reported a
		// nonsensical one). Treating that as already expired -- rather than caching the token
		// indefinitely -- is the conservative reading: refetching an extra time costs one round
		// trip, but caching a token past an expiry we were never told is a silent authentication
		// failure waiting to happen partway through a sync.
		t.expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
	}
	return t.token, nil
}

// oauth2TokenResponse is the standard OAuth2 client-credentials response body (RFC 6749 §5.1). The
// Iceberg REST specification's own /v1/oauth/tokens endpoint, Polaris and Snowflake Open Catalog
// all return this shape.
type oauth2TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

// fetchToken exchanges the configured client id and secret for a bearer token at t.tokenURL, per
// the client-credentials grant: a form-encoded POST carrying grant_type, client_secret and, if
// configured, client_id and scope. Both are omitted entirely when empty rather than sent blank.
// scope is optional in the OAuth2 specification. Omitting client_id is not merely tidy: Snowflake's
// Horizon Catalog authenticates on the secret alone and rejects an exchange carrying a client_id,
// reporting "invalid_scope" -- an error naming a field that is not the problem.
func (t *oauth2Transport) fetchToken(req *http.Request) (string, int64, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	if t.clientID != "" {
		form.Set("client_id", t.clientID)
	}
	form.Set("client_secret", t.clientSecret)
	if t.scope != "" {
		form.Set("scope", t.scope)
	}

	tokenReq, err := http.NewRequestWithContext(req.Context(), http.MethodPost, t.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("oauth2: failed to build token request for %s: %w", t.tokenURL, err)
	}
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	tokenClient := t.tokenClient
	if tokenClient == nil {
		tokenClient = http.DefaultClient
	}
	resp, err := tokenClient.Do(tokenReq)
	if err != nil {
		return "", 0, fmt.Errorf("oauth2: failed to reach token endpoint %s: %w", t.tokenURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("oauth2: failed to read token response from %s: %w", t.tokenURL, err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("oauth2: token endpoint %s returned status %d: %s", t.tokenURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload oauth2TokenResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, fmt.Errorf("oauth2: failed to parse token response from %s as JSON: %w", t.tokenURL, err)
	}
	if payload.AccessToken == "" {
		return "", 0, fmt.Errorf("oauth2: token endpoint %s returned no access_token", t.tokenURL)
	}

	return payload.AccessToken, payload.ExpiresIn, nil
}

// newOAuth2HTTPClient builds an HTTP client whose every request carries a current OAuth2
// client-credentials bearer token, obtained from tokenURL using clientID and the secret named by
// clientSecretEnvVar. headers, if non-empty, is attached to every request this client's transport
// chain issues -- including the token exchange itself, not only the catalog request the caller
// makes: see PropCatalogHeaderPrefix's doc comment in rest_auth.go for why the token endpoint needs
// the same treatment (it lives on the catalog's own host, unlike Entra ID's, which is Azure AD's).
//
// The secret is deliberately never a config property: a dataset config gets committed to git,
// logged, and POSTed to the REST service (the rule T51 and T55 settled for Azure credentials), and
// an OAuth2 client secret is exactly the kind of long-lived credential that rule exists to keep out
// of those places. clientSecretEnvVar instead *names* the environment variable holding the secret,
// mirroring AzureOptions.AccountKeyEnv: naming the variable rather than reading a single
// well-known one (there is no OAuth2-equivalent of AWS's credential chain or AZURE_STORAGE_KEY to
// fall back to) lets one process authenticate to several catalogs -- a Polaris deployment and a
// Snowflake Open Catalog account, say -- each from its own variable. An unset or empty variable is
// an error naming both the property and the variable, not a silent fall-through to an unauthenticated
// request, for the same reason resolveAzureCredential (pkg/io/azure.go) refuses to fall through.
func newOAuth2HTTPClient(timeout time.Duration, tokenURL, clientID, clientSecretEnvVar, scope string, headers http.Header) (*http.Client, error) {
	secret := os.Getenv(clientSecretEnvVar)
	if secret == "" {
		return nil, fmt.Errorf("oauth2 authentication for Iceberg REST catalog requires %q to name a non-empty environment variable holding the client secret; %s is unset or empty",
			PropCatalogOAuth2ClientSecretEnv, clientSecretEnvVar)
	}
	return NewOAuth2HTTPClientWithSecret(timeout, tokenURL, clientID, secret, scope, headers), nil
}

// NewOAuth2HTTPClientWithSecret builds the same client over a caller-supplied literal secret, which
// is how the tests drive it without setting an environment variable. Production code should prefer
// newOAuth2HTTPClient, which resolves the secret from the environment as T51/T55 require.
func NewOAuth2HTTPClientWithSecret(timeout time.Duration, tokenURL, clientID, clientSecret, scope string, headers http.Header) *http.Client {
	tokenClient := &http.Client{Timeout: timeout}
	if len(headers) > 0 {
		tokenClient.Transport = &headerTransport{headers: headers}
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &oauth2Transport{
			tokenClient:  tokenClient,
			tokenURL:     tokenURL,
			clientID:     clientID,
			clientSecret: clientSecret,
			scope:        scope,
		},
	}
}
