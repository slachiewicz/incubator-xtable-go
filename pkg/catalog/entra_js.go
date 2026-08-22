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

// ErrEntraUnsupported is returned by the WebAssembly build for any Entra ID authentication request.
// A browser sandbox has no Azure credential chain (no managed identity, no Azure CLI, no environment
// service principal) and the identity SDK is sizeable, so linking it there would add weight to serve
// code that can never run.
var ErrEntraUnsupported = errors.New("Entra ID authentication is unavailable in WebAssembly builds")

// newEntraHTTPClient always fails on WebAssembly. It exists so restHTTPClient keeps one shape across
// build targets and an auth=entra configuration reports a clear reason instead of failing to compile.
func newEntraHTTPClient(_ time.Duration, _ []string) (*http.Client, error) {
	return nil, ErrEntraUnsupported
}
