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

// DefaultOneLakeScope is the Entra ID scope requested when auth=entra and no scope is configured.
//
// The storage audience is what OneLake documents: it accepts tokens in the Storage audience only,
// which is the audience this scope requests. No request from this package has reached a Fabric
// workspace yet, and a non-OneLake REST catalog behind Entra ID may want a different audience, so
// PropCatalogScope stays configurable.
const DefaultOneLakeScope = "https://storage.azure.com/.default"

// restHTTPClient returns the HTTP client and static token a REST catalog client should use. Empty
// auth preserves today's behavior: a plain client and the static token in PropCatalogToken. auth
// values entra, entra-id and azure (case-insensitive, trimmed) instead build a client whose
// transport attaches a refreshed Entra ID bearer token, and the returned static token is empty
// since both REST client call sites already skip the Authorization header when the token is empty.
// Any other non-empty auth value is an error naming the value and the accepted ones, so a typo does
// not silently downgrade a deployment to unauthenticated.
func restHTTPClient(cfg *Config, timeout time.Duration) (*http.Client, string, error) {
	var props map[string]string
	if cfg != nil {
		props = cfg.Properties
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
	default:
		return nil, "", fmt.Errorf("unsupported %s %q for Iceberg REST catalog: accepted values are \"\" (static token), \"entra\", \"entra-id\" and \"azure\"", PropCatalogAuth, auth)
	}
}
