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

package io

import (
	"context"
	"errors"
)

// ErrGCSUnsupported is returned by the WebAssembly build for any Google Cloud Storage operation. A
// browser sandbox has no Application Default Credentials chain and no unrestricted network egress,
// so linking the GCS SDK there would add tens of megabytes to serve code that can never run.
var ErrGCSUnsupported = errors.New("Google Cloud Storage is unavailable in WebAssembly builds")

// GCSOptions mirrors the fields the non-wasm build exposes, so callers that construct option
// functions compile identically on both.
type GCSOptions struct {
	Endpoint        string
	AnonymousAccess bool
	CredentialsFile string
}

// NewGCSStorage always fails on WebAssembly. It exists so NewStorageForPath keeps one shape across
// build targets and a gs:// path reports a clear reason instead of failing to compile.
func NewGCSStorage(_ context.Context, _ ...func(*GCSOptions)) (Storage, error) {
	return nil, ErrGCSUnsupported
}
