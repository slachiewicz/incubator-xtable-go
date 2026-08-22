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

package io

import (
	"errors"
	"time"
)

// ErrCredentialsExpired reports that a caller-supplied static credential set (e.g. one vended by a
// catalog, see pkg/catalog.StorageCredentials) was already expired, or expired partway through use.
// It is distinct from the opaque AWS API error a request made with expired credentials would
// otherwise fail with (ExpiredToken and similar), so a long-running sync fails naming the actual
// cause instead of a generic S3 access error.
var ErrCredentialsExpired = errors.New("storage credentials expired")

// S3StaticCredentials supplies a fixed AWS access key, secret key and (for temporary credentials)
// session token, bypassing the default credential chain entirely. This is the shape a catalog vends
// short-lived credentials in: see pkg/catalog.StorageCredentials, which this is built from.
//
// This is untagged (not s3.go/s3_js.go) so both the real and the WebAssembly-stub S3Options can
// carry the same field without duplicating the struct definition.
type S3StaticCredentials struct {
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string
	// SessionToken is the AWS session token for temporary credentials. Empty for a long-lived,
	// directly-configured static credential.
	SessionToken string
	// Expiration is when the credentials stop being valid. The zero value means "no known
	// expiration" and Expired always reports false for it.
	Expiration time.Time
}

// Expired reports whether the credentials are expired, or within buffer of expiring, as of now. A
// zero Expiration is never expired: some static credentials (a directly-configured long-lived key
// pair, as opposed to one vended by a catalog) carry none.
func (c *S3StaticCredentials) Expired(now time.Time, buffer time.Duration) bool {
	if c == nil || c.Expiration.IsZero() {
		return false
	}
	return !c.Expiration.After(now.Add(buffer))
}
