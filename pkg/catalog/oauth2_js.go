//go:build js

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
	"errors"
	"net/http"
	"time"
)

// ErrOAuth2Unsupported is returned by the WebAssembly build for any OAuth2 client-credentials
// authentication request. This package requires the client secret to come from the environment
// (see newOAuth2HTTPClient), not a config property, exactly as T51 and T55 require for Azure
// credentials -- and a browser sandbox has no OS environment in that sense: there is no process
// boundary keeping a "server-side" secret out of the page that would read it. Disabling this mode
// in WebAssembly, the same way entra_js.go and sigv4_js.go disable their own credential chains,
// keeps that rule from becoming meaningless there instead of quietly relying on it anyway.
var ErrOAuth2Unsupported = errors.New("OAuth2 client-credentials authentication is unavailable in WebAssembly builds")

// newOAuth2HTTPClient always fails on WebAssembly. It exists so restHTTPClient keeps one shape
// across build targets and an auth=oauth2 configuration reports a clear reason instead of failing
// to compile.
func newOAuth2HTTPClient(_ time.Duration, _, _, _, _ string, _ http.Header) (*http.Client, error) {
	return nil, ErrOAuth2Unsupported
}
