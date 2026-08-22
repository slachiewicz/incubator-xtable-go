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

package io_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

// testAzureURI addresses an account whose host swaps cleanly to a blob endpoint; NewAzureStorage
// makes no network call during construction, so every case below succeeds or fails purely on
// credential resolution.
const testAzureURI = "abfss://container@acct.dfs.core.windows.net/path"

// validAzureAccountKey is a syntactically valid (base64-decodable) shared key — the well-known
// Azurite development key, also used in docs/azure.md. azblob.NewSharedKeyCredential rejects
// anything that does not base64-decode, which is what makes it useful as a test oracle: an
// invalid string here proves the shared-key path was actually reached and its value consumed.
const validAzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

// invalidAzureAccountKey does not base64-decode, so azblob.NewSharedKeyCredential rejects it.
// It stands in for "this value must not be the one that got used."
const invalidAzureAccountKey = "not-valid-base64!!!"

// TestNewAzureStorage_CredentialResolutionOrder exercises the precedence between a literal
// AzureOptions field, a named environment variable (AccountKeyEnv / SASTokenEnv), and the
// well-known AZURE_STORAGE_KEY / AZURE_STORAGE_SAS_TOKEN variables. Every subtest blanks both
// well-known variables first, since a developer machine or CI runner may already export them and
// the "unchanged behavior" cases are only hermetic if the test controls both.
//
// t.Setenv forbids t.Parallel in the same test (and its ancestors), so neither this test nor its
// subtests call it.
func TestNewAzureStorage_CredentialResolutionOrder(t *testing.T) {
	tests := []struct {
		name string
		opts func(*io.AzureOptions)
		env  map[string]string

		wantErr         bool
		wantErrContains string
	}{
		{
			name: "no new fields set behaves exactly as before: well-known account key wins",
			opts: func(o *io.AzureOptions) {},
			env: map[string]string{
				"AZURE_STORAGE_KEY": validAzureAccountKey,
			},
			wantErr: false,
		},
		{
			name: "no new fields set behaves exactly as before: invalid well-known account key still fails",
			opts: func(o *io.AzureOptions) {},
			env: map[string]string{
				"AZURE_STORAGE_KEY": invalidAzureAccountKey,
			},
			wantErr: true,
		},
		{
			name: "named account key variable set and used",
			opts: func(o *io.AzureOptions) {
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY"
			},
			env: map[string]string{
				"ACCT1_STORAGE_KEY": validAzureAccountKey,
			},
			wantErr: false,
		},
		{
			name: "named account key variable set but empty is an error naming the variable",
			opts: func(o *io.AzureOptions) {
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY"
			},
			env: map[string]string{
				"ACCT1_STORAGE_KEY": "",
			},
			wantErr:         true,
			wantErrContains: "ACCT1_STORAGE_KEY",
		},
		{
			name: "named account key variable unset is an error naming the variable",
			opts: func(o *io.AzureOptions) {
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY_NEVER_SET"
			},
			env:             nil,
			wantErr:         true,
			wantErrContains: "ACCT1_STORAGE_KEY_NEVER_SET",
		},
		{
			name: "named account key variable outranks the well-known one",
			opts: func(o *io.AzureOptions) {
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY"
			},
			env: map[string]string{
				"ACCT1_STORAGE_KEY": validAzureAccountKey,
				"AZURE_STORAGE_KEY": invalidAzureAccountKey,
			},
			wantErr: false,
		},
		{
			name: "a literal AccountKey outranks both the named and well-known variables",
			opts: func(o *io.AzureOptions) {
				o.AccountKey = validAzureAccountKey
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY"
			},
			env: map[string]string{
				"ACCT1_STORAGE_KEY": invalidAzureAccountKey,
				"AZURE_STORAGE_KEY": invalidAzureAccountKey,
			},
			wantErr: false,
		},
		{
			// This is the exact failure the no-fall-through rule exists to prevent: a broken
			// named variable must not quietly fall back to a perfectly valid well-known one.
			name: "an unset named account key variable errors even though the well-known one is valid",
			opts: func(o *io.AzureOptions) {
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY_NEVER_SET"
			},
			env: map[string]string{
				"AZURE_STORAGE_KEY": validAzureAccountKey,
			},
			wantErr:         true,
			wantErrContains: "ACCT1_STORAGE_KEY_NEVER_SET",
		},
		{
			name: "named SAS token variable set and used",
			opts: func(o *io.AzureOptions) {
				o.SASTokenEnv = "ACCT1_SAS_TOKEN"
			},
			env: map[string]string{ //nolint:gosec // fixture value, not a real SAS token
				"ACCT1_SAS_TOKEN": "sv=2021-08-06&sig=example",
			},
			wantErr: false,
		},
		{
			name: "named SAS token variable set but empty is an error naming the variable",
			opts: func(o *io.AzureOptions) {
				o.SASTokenEnv = "ACCT1_SAS_TOKEN"
			},
			env: map[string]string{
				"ACCT1_SAS_TOKEN": "",
			},
			wantErr:         true,
			wantErrContains: "ACCT1_SAS_TOKEN",
		},
		{
			name: "named SAS token variable unset is an error naming the variable",
			opts: func(o *io.AzureOptions) {
				o.SASTokenEnv = "ACCT1_SAS_TOKEN_NEVER_SET"
			},
			env:             nil,
			wantErr:         true,
			wantErrContains: "ACCT1_SAS_TOKEN_NEVER_SET",
		},
		{
			name: "named SAS token variable outranks the well-known one",
			opts: func(o *io.AzureOptions) {
				o.SASTokenEnv = "ACCT1_SAS_TOKEN"
			},
			env: map[string]string{ //nolint:gosec // fixture values, not real credentials
				"ACCT1_SAS_TOKEN":         "sv=2021-08-06&sig=example",
				"AZURE_STORAGE_SAS_TOKEN": "sv=irrelevant",
				"AZURE_STORAGE_KEY":       invalidAzureAccountKey,
			},
			wantErr: false,
		},
		{
			name: "SAS outranks a shared key, named or well-known: an invalid key never gets read",
			opts: func(o *io.AzureOptions) {
				o.SASTokenEnv = "ACCT1_SAS_TOKEN"
				o.AccountKeyEnv = "ACCT1_STORAGE_KEY"
			},
			env: map[string]string{ //nolint:gosec // fixture values, not real credentials
				"ACCT1_SAS_TOKEN":   "sv=2021-08-06&sig=example",
				"ACCT1_STORAGE_KEY": invalidAzureAccountKey,
			},
			wantErr: false,
		},
		{
			name: "a literal SASToken outranks both the named and well-known variables",
			opts: func(o *io.AzureOptions) {
				o.SASToken = "sv=2021-08-06&sig=example"
				o.SASTokenEnv = "ACCT1_SAS_TOKEN"
			},
			env: map[string]string{
				"ACCT1_SAS_TOKEN":         "",
				"AZURE_STORAGE_SAS_TOKEN": "",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// t.Setenv forbids t.Parallel in this test or any ancestor, so none is called here.
			t.Setenv("AZURE_STORAGE_SAS_TOKEN", "")
			t.Setenv("AZURE_STORAGE_KEY", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			_, err := io.NewAzureStorage(context.Background(), testAzureURI, tt.opts)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}
