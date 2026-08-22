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
	"strconv"
	"strings"
	"time"
)

// AccessDelegationHeader and AccessDelegationVendedCredentials are the Iceberg REST header and
// value that ask a catalog to return storage credentials scoped to the table, alongside its
// metadata, instead of requiring the caller to hold bucket-wide credentials of its own. Observed
// against a live Snowflake-managed Iceberg table on 2026-08-22: without it, the table's data sits
// in a bucket no customer holds (or can be issued) credentials for, and every read fails.
const (
	AccessDelegationHeader = "X-Iceberg-Access-Delegation"
	//nolint:gosec // this is the specification's header value, not a credential
	AccessDelegationVendedCredentials = "vended-credentials"
)

// Iceberg REST config keys carrying vended AWS credentials. These names are shared between the two
// response shapes this file parses: the flat top-level "config" map, and each entry's "config" map
// inside "storage-credentials".
//
// The first five below (through vendedConfigKeyClientRegion) are observed keys: a live Snowflake
// catalog returned exactly this set on 2026-08-22 in the flat "config" shape. vendedConfigKeyExpiresAtMs
// is not observed against a real server; it is the Iceberg REST specification's documented key for
// the same value (the alternative to "expiration-time" the specification names for AWS credentials
// specifically), included defensively since a spec-conforming server may use it instead.
const (
	vendedConfigKeyAccessKeyID     = "s3.access-key-id"
	vendedConfigKeySecretAccessKey = "s3.secret-access-key"
	vendedConfigKeySessionToken    = "s3.session-token"
	vendedConfigKeyExpirationTime  = "expiration-time"
	vendedConfigKeyClientRegion    = "client.region"
	vendedConfigKeyExpiresAtMs     = "s3.session-token-expires-at-ms"
)

// StorageCredentials carries the short-lived, scoped storage credentials an Iceberg REST catalog
// vends alongside a table's metadata when asked with AccessDelegationHeader. It is a struct with
// named fields rather than the raw map the wire format uses, so a call site cannot mistake the
// session token for the secret key or forget to carry the region that a bucket in a
// non-default region requires.
//
// StorageCredentials must never be logged, written to a config file, or included in an error
// message: the fields are live, working credentials. String, GoString and MarshalJSON are all
// overridden to redact them, since %v, %+v, %#v and encoding/json all reach a type through
// different methods and each one is a leak if left to the default.
type StorageCredentials struct {
	// AccessKeyID is the vended AWS access key ID.
	AccessKeyID string
	// SecretAccessKey is the vended AWS secret access key.
	SecretAccessKey string
	// SessionToken is the vended AWS session token.
	SessionToken string
	// Region is the AWS region the credentials (and the bucket behind them) belong to. It is not
	// an optional extra: a vended credential without its region is exactly what produces a
	// PermanentRedirect against a bucket outside the client's default region.
	Region string
	// Expiration is when the credentials stop being valid. The zero value means the catalog
	// response carried no expiration, which Expired always treats as "not expired" -- callers
	// that need to require an expiration say so themselves.
	Expiration time.Time
}

// redactedStorageCredentials is what String, GoString and MarshalJSON all render, regardless of
// which fields are populated -- the point is that none of them can be recovered from the output.
const redactedStorageCredentials = "catalog.StorageCredentials{REDACTED}"

// String implements fmt.Stringer, covering the %s and %v verbs.
func (c StorageCredentials) String() string { return redactedStorageCredentials }

// GoString implements fmt.GoStringer, covering the %#v verb, which fmt.Stringer alone does not.
func (c StorageCredentials) GoString() string { return redactedStorageCredentials }

// MarshalJSON redacts the credentials if a caller ever serializes a value holding them, e.g. by
// embedding SourceTable in a larger structure and marshaling that. It intentionally does not round
// -trip: the whole point is that the secret does not survive being written out.
func (c StorageCredentials) MarshalJSON() ([]byte, error) {
	return []byte(`"REDACTED"`), nil
}

// Expired reports whether the credentials are expired, or within buffer of expiring, as of now. A
// zero Expiration (no expiration reported) is never expired.
func (c StorageCredentials) Expired(now time.Time, buffer time.Duration) bool {
	if c.Expiration.IsZero() {
		return false
	}
	return !c.Expiration.After(now.Add(buffer))
}

// parseStorageCredentials resolves the credentials for basePath out of a load-table response,
// preferring the "storage-credentials" array (the current Iceberg REST specification shape) over
// the flat "config" map (the older shape, and the one observed against Snowflake) when both are
// present. It returns (nil, nil) when the response carries neither -- a catalog that does not vend
// credentials at all, which must leave the caller's existing behavior unchanged.
func parseStorageCredentials(basePath string, storageCredentials []loadTableStorageCredential, config map[string]string) *StorageCredentials {
	if creds := bestMatchingStorageCredential(basePath, storageCredentials); creds != nil {
		// The two blocks are complementary, not alternatives, and treating them as alternatives is
		// a bug that only shows against a real server. Snowflake returns a storage-credentials
		// entry carrying the key, secret, session token and expiry -- and no "client.region", which
		// appears only in the top-level config. Preferring the array wholesale therefore yields a
		// credential with no region, and the very PermanentRedirect this field exists to prevent.
		//
		// So: the array scopes the credential to a prefix; the top-level config carries table-wide
		// client settings. Fill anything the chosen entry left empty from the latter.
		if fallback := credentialsFromConfig(config); fallback != nil {
			if creds.Region == "" {
				creds.Region = fallback.Region
			}
			if creds.Expiration.IsZero() {
				creds.Expiration = fallback.Expiration
			}
		}
		if creds.Region == "" {
			creds.Region = strings.TrimSpace(config[vendedConfigKeyClientRegion])
		}
		return creds
	}
	return credentialsFromConfig(config)
}

// bestMatchingStorageCredential picks the entry whose prefix is the longest match for basePath, per
// the Iceberg REST specification: a client holding several scoped credentials picks the most
// specific one. An entry with an empty prefix matches every location. Returns nil when the array is
// empty or nothing in it carries a usable credential.
func bestMatchingStorageCredential(basePath string, storageCredentials []loadTableStorageCredential) *StorageCredentials {
	var best *StorageCredentials
	bestPrefixLen := -1
	for _, entry := range storageCredentials {
		if entry.Prefix != "" && !strings.HasPrefix(basePath, entry.Prefix) {
			continue
		}
		creds := credentialsFromConfig(entry.Config)
		if creds == nil {
			continue
		}
		if len(entry.Prefix) > bestPrefixLen {
			best = creds
			bestPrefixLen = len(entry.Prefix)
		}
	}
	return best
}

// credentialsFromConfig extracts a StorageCredentials from a single flat config map, as either the
// top-level "config" block or one storage-credentials entry's "config" carries it. Returns nil when
// the map carries none of the AWS credential keys at all, which is the normal case for a catalog
// that does not vend credentials.
func credentialsFromConfig(config map[string]string) *StorageCredentials {
	if config == nil {
		return nil
	}
	accessKeyID := strings.TrimSpace(config[vendedConfigKeyAccessKeyID])
	secretAccessKey := strings.TrimSpace(config[vendedConfigKeySecretAccessKey])
	if accessKeyID == "" || secretAccessKey == "" {
		return nil
	}

	return &StorageCredentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    strings.TrimSpace(config[vendedConfigKeySessionToken]),
		Region:          strings.TrimSpace(config[vendedConfigKeyClientRegion]),
		Expiration:      parseExpiration(config),
	}
}

// parseExpiration reads the credential expiration out of a config map. The wire format for the
// value is not documented anywhere this could be verified against: the observed Snowflake response
// carries the "expiration-time" key but this could not be checked against a real value, so both an
// RFC3339 timestamp and an integer count of milliseconds since the Unix epoch are accepted, tried in
// that order, under either of the two known key names. An unparsable or absent value yields the zero
// time, which Expired treats as "no known expiration" rather than an error: a catalog carrying
// credentials this code cannot parse the expiration of should not become entirely unusable for it.
func parseExpiration(config map[string]string) time.Time {
	for _, key := range []string{vendedConfigKeyExpirationTime, vendedConfigKeyExpiresAtMs} {
		raw := strings.TrimSpace(config[key])
		if raw == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t
		}
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return time.UnixMilli(ms)
		}
	}
	return time.Time{}
}
