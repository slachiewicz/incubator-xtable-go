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

// Package io_test covers docs/improvement-plan.md T64: accepting a catalog-vended static AWS
// credential set instead of the default credential chain, and never silently reading (or writing)
// with one that has expired.
package io_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

// TestNewS3Storage_NoCredentials proves the zero-value S3Options (what every caller not using a
// vending catalog supplies) is unaffected: construction never even looks at opts.Credentials.
func TestNewS3Storage_NoCredentials(t *testing.T) {
	t.Parallel()

	storage, err := io.NewS3Storage(t.Context(), func(o *io.S3Options) {
		o.Region = "us-east-1"
	})
	require.NoError(t, err)
	require.NotNil(t, storage)
}

// TestNewS3Storage_ExpiredCredentials covers construction-time rejection of an already-expired (or
// near-expired) credential set: a long sync must never silently start with one, per T64's "do not
// silently proceed with an expired credential."
func TestNewS3Storage_ExpiredCredentials(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name        string
		expiration  time.Time
		expectError bool
	}{
		{name: "already expired", expiration: now.Add(-time.Hour), expectError: true},
		{name: "expires within the clock-skew buffer", expiration: now.Add(30 * time.Second), expectError: true},
		{name: "safely valid", expiration: now.Add(time.Hour), expectError: false},
		{name: "no expiration reported", expiration: time.Time{}, expectError: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			storage, err := io.NewS3Storage(t.Context(), func(o *io.S3Options) {
				o.Region = "us-east-1"
				o.Credentials = &io.S3StaticCredentials{
					AccessKeyID:     "AKIATEST",
					SecretAccessKey: "test-secret",
					Expiration:      tt.expiration,
				}
			})

			if tt.expectError {
				require.Error(t, err)
				assert.ErrorIs(t, err, io.ErrCredentialsExpired)
				assert.Nil(t, storage)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, storage)
		})
	}
}

// fakeS3ErrorServer answers every request with an S3-shaped XML error, so a request built with
// static credentials can be pointed at it via S3Options.Endpoint without touching real AWS.
func fakeS3ErrorServer(t *testing.T, statusCode int, errorCode string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>` + errorCode + `</Code><Message>test fixture error</Message><RequestId>test</RequestId></Error>`))
	}))
	t.Cleanup(server.Close)
	return server
}

// TestS3Storage_ExpiredTokenTranslatedToNamedError covers the mid-sync case: credentials that were
// valid at construction time but have since expired against S3's own clock produce
// io.ErrCredentialsExpired, not an opaque, unwrapped AWS API error.
func TestS3Storage_ExpiredTokenTranslatedToNamedError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		errorCode string
		operation func(ctx context.Context, s *io.S3Storage) error
	}{
		{
			name:      "List",
			errorCode: "ExpiredToken",
			operation: func(ctx context.Context, s *io.S3Storage) error {
				_, err := s.List(ctx, "s3://bucket/prefix")
				return err
			},
		},
		{
			name:      "Read",
			errorCode: "ExpiredToken",
			operation: func(ctx context.Context, s *io.S3Storage) error {
				_, err := s.Read(ctx, "s3://bucket/key")
				return err
			},
		},
		{
			name:      "RequestExpired code",
			errorCode: "RequestExpired",
			operation: func(ctx context.Context, s *io.S3Storage) error {
				_, err := s.Read(ctx, "s3://bucket/key")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := fakeS3ErrorServer(t, http.StatusBadRequest, tt.errorCode)

			storage, err := io.NewS3Storage(t.Context(), func(o *io.S3Options) {
				o.Region = "us-east-1"
				o.Endpoint = server.URL
				o.UsePathStyle = true
				o.Credentials = &io.S3StaticCredentials{
					AccessKeyID:     "AKIATEST",
					SecretAccessKey: "test-secret",
					SessionToken:    "test-session-token",
				}
			})
			require.NoError(t, err)

			err = tt.operation(t.Context(), storage)
			require.Error(t, err)
			assert.True(t, errors.Is(err, io.ErrCredentialsExpired),
				"expected error to wrap ErrCredentialsExpired, got: %v", err)
		})
	}
}
