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
	"net/url"
	"strings"
	"time"
)

// PropCatalogAuth selects the authentication mode for a REST catalog. The default, an empty value,
// is the static bearer token in PropCatalogToken.
const PropCatalogAuth = "auth"

// PropCatalogToken carries a static bearer token.
const PropCatalogToken = "token"

// PropCatalogScope overrides the OAuth2 scope requested for the catalog, for both auth modes that
// speak an OAuth2-shaped token request: entra/entra-id/azure and oauth2. The two differ in what an
// empty value means. For entra, empty falls back to DefaultOneLakeScope, because Entra ID always
// requires a scope. For oauth2, empty omits the scope form field entirely -- it is optional in the
// OAuth2 client-credentials grant -- rather than defaulting to any one catalog's convention; Apache
// Polaris's own examples request "PRINCIPAL_ROLE:ALL", but that is Polaris-specific role-scoping,
// not part of the specification, so it belongs here as documentation, not as this package's default.
const PropCatalogScope = "scope"

// PropCatalogOAuth2ClientID is the OAuth2 client-credentials client id (auth=oauth2 only). Unlike
// the client secret, this is not sensitive, so it may be an ordinary config property.
const PropCatalogOAuth2ClientID = "clientId"

// PropCatalogOAuth2ClientSecretEnv names the environment variable holding the OAuth2 client secret
// (auth=oauth2 only). See newOAuth2HTTPClient's doc comment for why the secret itself is never a
// config property.
const PropCatalogOAuth2ClientSecretEnv = "clientSecretEnv"

// PropCatalogOAuth2TokenEndpoint overrides the OAuth2 token endpoint (auth=oauth2 only). The
// Iceberg REST specification places it at POST <catalog-uri>/v1/oauth/tokens, which is the default
// used when this is unset; a deployment that moves the endpoint elsewhere can override it here.
const PropCatalogOAuth2TokenEndpoint = "oauth2TokenEndpoint" //nolint:gosec // a property name constant, not a credential

// PropCatalogHeaderPrefix marks a config property as an extra HTTP header attached to every request
// a REST catalog client sends, independent of the auth mode in use: a property named
// "header.<Name>" with value "<value>" attaches header "<Name>: <value>". This is a prefix on
// arbitrary property keys, rather than one property packing several headers into a single string
// (as a comma- or semicolon-joined value would), so that a header value containing a comma, a
// semicolon or a colon needs no escaping -- map[string]string already handles that. Apache
// Iceberg's own REST catalog clients use the same "header.<Name>" property convention, so a config
// already written for one of them carries over unchanged.
//
// Apache Polaris is the motivating case: reaching it required a "Polaris-Realm: POLARIS" header
// (confirmed against a local Polaris container) that selects which realm a request addresses, a
// concern OAuth2 alone cannot express. Extra headers are attached after whatever auth transport is
// in use signs or authenticates the request -- see headerTransport below -- so for auth=sigv4 they
// are also covered by the signature (in SignedHeaders), not layered on unsigned afterward.
//
// For auth=oauth2, these headers are attached to the token exchange (POST /v1/oauth/tokens) as well
// as to the catalog request: the token endpoint is on the catalog's own host, so a realm header
// Polaris needs to route the catalog request typically applies to the token request too. Entra ID's
// token endpoint has no analogous need -- it is Azure AD's own host, not the catalog's -- and
// sigv4 has no token endpoint to attach anything to.
const PropCatalogHeaderPrefix = "header."

// PropCatalogWarehouse carries the Iceberg REST warehouse identifier sent as
// GET /v1/config?warehouse=<value>. This is distinct from Config.DatabaseName, which maps to the
// Iceberg namespace (e.g. "dbo" in Microsoft's OneLake examples): a warehouse addresses the whole
// catalog instance (OneLake's is "<WorkspaceID>/<DataItemID>"), a namespace a grouping within it.
const PropCatalogWarehouse = "warehouse"

// DefaultOneLakeScope is the Entra ID scope requested when auth=entra and no scope is configured.
//
// The storage audience is what OneLake documents: it accepts tokens in the Storage audience only,
// which is the audience this scope requests. No request from this package has reached a Fabric
// workspace yet, and a non-OneLake REST catalog behind Entra ID may want a different audience, so
// PropCatalogScope stays configurable.
const DefaultOneLakeScope = "https://storage.azure.com/.default"

// PropCatalogRegion is the AWS region SigV4 requests are signed for (auth=sigv4/aws only). When
// unset, it is read off the catalog URI's host if that host is shaped like
// <service>.<region>.amazonaws.com -- both of AWS's native Iceberg REST endpoints are (Glue's
// glue.<region>.amazonaws.com, S3 Tables' s3tables.<region>.amazonaws.com) -- and otherwise falls
// back to the AWS SDK's own region resolution (AWS_REGION, AWS_DEFAULT_REGION, the shared config
// file). Unlike PropCatalogSigningName below, a wrong region only produces an authentication
// failure the server reports plainly, so this generic fallback chain is an acceptable default.
const PropCatalogRegion = "region"

// PropCatalogSigningName is the SigV4 signing name -- what AWS calls the "service" in a credential
// scope -- and is required for auth=sigv4/aws unless it can be read off the URI host the same way
// PropCatalogRegion is. It is "glue" for Glue's Iceberg REST endpoint and "s3tables" for S3
// Tables'. There is no safe generic default the way there is for region: signing with the wrong
// service name produces a request that is well-formed and still rejected by the server, which is a
// worse failure mode than refusing to build the client at all.
const PropCatalogSigningName = "signingName"

// restHTTPClient returns the HTTP client and static token a REST catalog client should use. It
// builds the base client for the configured auth mode (buildRESTAuthClient) and then, regardless of
// that mode, wraps it with any extra headers configured via PropCatalogHeaderPrefix.
func restHTTPClient(cfg *Config, timeout time.Duration) (*http.Client, string, error) {
	var props map[string]string
	var uri string
	if cfg != nil {
		props = cfg.Properties
		uri = cfg.URI
	}

	client, token, err := buildRESTAuthClient(props, uri, timeout)
	if err != nil {
		return nil, "", err
	}

	headers := extraHeaders(props)
	if len(headers) > 0 {
		client.Transport = &headerTransport{base: client.Transport, headers: headers}
	}
	return client, token, nil
}

// buildRESTAuthClient builds the HTTP client for cfg's auth mode, before any extra headers are
// applied. Empty auth preserves today's behavior: a plain client and the static token in
// PropCatalogToken. auth values entra, entra-id and azure (case-insensitive, trimmed) instead build
// a client whose transport attaches a refreshed Entra ID bearer token; sigv4/aws build one whose
// transport SigV4-signs every request; oauth2/oauth/client-credentials build one whose transport
// attaches an OAuth2 client-credentials bearer token, refreshed the same way. All three non-empty
// modes return an empty static token since every REST client call site already skips the
// Authorization header when the token is empty. Any other non-empty auth value is an error naming
// the value and the accepted ones, so a typo does not silently downgrade a deployment to
// unauthenticated.
func buildRESTAuthClient(props map[string]string, uri string, timeout time.Duration) (*http.Client, string, error) {
	auth := strings.TrimSpace(props[PropCatalogAuth])
	switch strings.ToLower(auth) {
	case "":
		return &http.Client{Timeout: timeout}, props[PropCatalogToken], nil
	case "entra", "entra-id", "azure":
		scope := strings.TrimSpace(props[PropCatalogScope])
		if scope == "" {
			scope = DefaultOneLakeScope
		}
		client, err := newEntraHTTPClient(timeout, []string{scope})
		if err != nil {
			return nil, "", err
		}
		return client, "", nil
	case "sigv4", "aws":
		region := strings.TrimSpace(props[PropCatalogRegion])
		signingName := strings.TrimSpace(props[PropCatalogSigningName])
		if region == "" || signingName == "" {
			derivedRegion, derivedSigningName := deriveSigV4HostDefaults(uri)
			if region == "" {
				region = derivedRegion
			}
			if signingName == "" {
				signingName = derivedSigningName
			}
		}
		if signingName == "" {
			return nil, "", fmt.Errorf("%s %q for Iceberg REST catalog requires %q: it could not be derived from URI %q (expected a host shaped like <service>.<region>.amazonaws.com, e.g. Glue's glue.<region>.amazonaws.com or S3 Tables' s3tables.<region>.amazonaws.com)",
				PropCatalogAuth, auth, PropCatalogSigningName, uri)
		}

		client, err := newSigV4HTTPClient(timeout, region, signingName)
		if err != nil {
			return nil, "", err
		}
		return client, "", nil
	case "oauth2", "oauth", "client-credentials":
		clientID := strings.TrimSpace(props[PropCatalogOAuth2ClientID])
		if clientID == "" {
			return nil, "", fmt.Errorf("%s %q for Iceberg REST catalog requires %q", PropCatalogAuth, auth, PropCatalogOAuth2ClientID)
		}
		clientSecretEnvVar := strings.TrimSpace(props[PropCatalogOAuth2ClientSecretEnv])
		if clientSecretEnvVar == "" {
			return nil, "", fmt.Errorf("%s %q for Iceberg REST catalog requires %q, naming the environment variable that holds the client secret -- the secret itself must never be a config property",
				PropCatalogAuth, auth, PropCatalogOAuth2ClientSecretEnv)
		}
		tokenEndpoint := strings.TrimSpace(props[PropCatalogOAuth2TokenEndpoint])
		if tokenEndpoint == "" {
			tokenEndpoint = defaultOAuth2TokenEndpoint(uri)
		}
		if tokenEndpoint == "" {
			return nil, "", fmt.Errorf("%s %q for Iceberg REST catalog requires %q: it could not be derived from an empty catalog URI",
				PropCatalogAuth, auth, PropCatalogOAuth2TokenEndpoint)
		}
		scope := strings.TrimSpace(props[PropCatalogScope])

		// Passed through to the token exchange itself, not just the catalog request restHTTPClient
		// wraps afterward: see PropCatalogHeaderPrefix's doc comment for why.
		client, err := newOAuth2HTTPClient(timeout, tokenEndpoint, clientID, clientSecretEnvVar, scope, extraHeaders(props))
		if err != nil {
			return nil, "", err
		}
		return client, "", nil
	default:
		return nil, "", fmt.Errorf("unsupported %s %q for Iceberg REST catalog: accepted values are \"\" (static token), \"entra\", \"entra-id\", \"azure\", \"sigv4\", \"aws\", \"oauth2\", \"oauth\" and \"client-credentials\"", PropCatalogAuth, auth)
	}
}

// defaultOAuth2TokenEndpoint derives the OAuth2 token endpoint the Iceberg REST specification
// places at POST <catalog-uri>/v1/oauth/tokens. It returns "" for an empty uri, in which case the
// caller must be configured with PropCatalogOAuth2TokenEndpoint explicitly.
func defaultOAuth2TokenEndpoint(uri string) string {
	uri = strings.TrimRight(strings.TrimSpace(uri), "/")
	if uri == "" {
		return ""
	}
	return uri + "/v1/oauth/tokens"
}

// headerTransport is an http.RoundTripper that attaches a fixed set of extra headers to every
// request, then delegates to base. It never mutates req, the same discipline entraTransport,
// sigv4Transport and oauth2Transport all follow: it clones the request and sets headers on the
// clone.
type headerTransport struct {
	base    http.RoundTripper
	headers http.Header
}

// RoundTrip implements http.RoundTripper.
func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	for name, values := range t.headers {
		for _, value := range values {
			clone.Header.Add(name, value)
		}
	}

	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

// extraHeaders collects every "header.<Name>": "<value>" property into an http.Header, stripping
// the PropCatalogHeaderPrefix. It returns an empty, non-nil header when there are none, so callers
// can check len() without a nil check.
func extraHeaders(props map[string]string) http.Header {
	headers := make(http.Header)
	for key, value := range props {
		name, ok := strings.CutPrefix(key, PropCatalogHeaderPrefix)
		if !ok || name == "" {
			continue
		}
		headers.Add(name, value)
	}
	return headers
}

// deriveSigV4HostDefaults reads the region and signing name out of a URI host shaped like
// <service>.<region>.amazonaws.com, the form both of AWS's native Iceberg REST endpoints use. Both
// return values are empty when the host is not shaped this way, which is the expected outcome for
// any catalog other than Glue's or S3 Tables' -- callers must then set PropCatalogRegion and/or
// PropCatalogSigningName explicitly.
func deriveSigV4HostDefaults(rawURI string) (region, signingName string) {
	parsed, err := url.Parse(rawURI)
	if err != nil || parsed.Host == "" {
		return "", ""
	}

	labels := strings.Split(parsed.Hostname(), ".")
	if len(labels) != 4 || labels[2] != "amazonaws" || labels[3] != "com" {
		return "", ""
	}
	return labels[1], labels[0]
}
