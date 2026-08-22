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

// ErrAzureUnsupported is returned by the WebAssembly build for any Azure Blob Storage operation. A
// browser sandbox has no Azure credential chain and no unrestricted network egress, so linking the
// Azure SDK there would add tens of megabytes to serve code that can never run.
var ErrAzureUnsupported = errors.New("Azure storage is unavailable in WebAssembly builds")

// AzureOptions mirrors the fields the non-wasm build exposes, so callers that construct option
// functions compile identically on both. CustomHTTPClient is omitted: it exists only to inject an
// HTTP transport, which has no meaning here.
type AzureOptions struct {
	Endpoint      string
	AccountName   string
	AccountKey    string
	AccountKeyEnv string
	SASToken      string
	SASTokenEnv   string
	Anonymous     bool
}

// NewAzureStorage always fails on WebAssembly. It exists so NewStorageForPath keeps one shape
// across build targets and an abfss:// path reports a clear reason instead of failing to compile.
func NewAzureStorage(_ context.Context, _ string, _ ...func(*AzureOptions)) (Storage, error) {
	return nil, ErrAzureUnsupported
}
