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

// PropCatalogScope overrides the Entra ID scope requested for the catalog.
const PropCatalogScope = "scope"

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

// restHTTPClient returns the HTTP client and static token a REST catalog client should use. Empty
// auth preserves today's behavior: a plain client and the static token in PropCatalogToken. auth
// values entra, entra-id and azure (case-insensitive, trimmed) instead build a client whose
// transport attaches a refreshed Entra ID bearer token, and sigv4/aws build one whose transport
// SigV4-signs every request; both return an empty static token since every REST client call site
// already skips the Authorization header when the token is empty. Any other non-empty auth value
// is an error naming the value and the accepted ones, so a typo does not silently downgrade a
// deployment to unauthenticated.
func restHTTPClient(cfg *Config, timeout time.Duration) (*http.Client, string, error) {
	var props map[string]string
	var uri string
	if cfg != nil {
		props = cfg.Properties
		uri = cfg.URI
	}

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
	default:
		return nil, "", fmt.Errorf("unsupported %s %q for Iceberg REST catalog: accepted values are \"\" (static token), \"entra\", \"entra-id\", \"azure\", \"sigv4\" and \"aws\"", PropCatalogAuth, auth)
	}
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
