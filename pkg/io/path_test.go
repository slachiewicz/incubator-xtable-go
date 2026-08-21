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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

func TestRelativizePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		file     string
		base     string
		want     string
		wantErr  error
		errWords []string
	}{
		{
			name: "unqualified on both sides",
			file: "/data/events/data/part-0.parquet",
			base: "/data/events",
			want: "data/part-0.parquet",
		},
		{
			name: "base with a trailing slash",
			file: "/data/events/data/part-0.parquet",
			base: "/data/events/",
			want: "data/part-0.parquet",
		},
		{
			name: "scheme on the file only",
			file: "file:///data/events/data/part-0.parquet",
			base: "/data/events",
			want: "data/part-0.parquet",
		},
		{
			name: "scheme on the base only",
			file: "/data/events/data/part-0.parquet",
			base: "file:///data/events",
			want: "data/part-0.parquet",
		},
		{
			name: "scheme on both sides",
			file: "file:///data/events/data/part-0.parquet",
			base: "file:///data/events/",
			want: "data/part-0.parquet",
		},
		{
			name: "s3 bucket path",
			file: "s3://bucket/warehouse/events/data/part-0.parquet",
			base: "s3://bucket/warehouse/events",
			want: "data/part-0.parquet",
		},
		{
			name: "bucket root as the base",
			file: "s3://bucket/part-0.parquet",
			base: "s3://bucket",
			want: "part-0.parquet",
		},
		{
			name: "s3a and s3 name the same store",
			file: "s3a://bucket/events/part-0.parquet",
			base: "s3://bucket/events",
			want: "part-0.parquet",
		},
		{
			name: "filesystem root as the base",
			file: "/part-0.parquet",
			base: "/",
			want: "part-0.parquet",
		},
		{
			name: "duplicate separators collapse",
			file: "file:///data/events//data/part-0.parquet",
			base: "/data//events",
			want: "data/part-0.parquet",
		},
		{
			name: "a nested partition directory survives whole",
			file: "file:///data/events/region=eu/day=1/part-0.parquet",
			base: "/data/events",
			want: "region=eu/day=1/part-0.parquet",
		},
		{
			// model.DataFile allows a relative PhysicalPath, and the targets record exactly this form.
			name: "a path that is already relative",
			file: "data/part-0.parquet",
			base: "mem://test",
			want: "data/part-0.parquet",
		},
		{
			name: "a bare file name is already relative",
			file: "part-0.parquet",
			base: "/data/events",
			want: "part-0.parquet",
		},
		{
			name:    "a relative path that climbs out of the base",
			file:    "../other/part-0.parquet",
			base:    "/data/events",
			wantErr: io.ErrPathNotUnderBase,
		},
		{
			name:     "a sibling directory sharing the prefix is not under the base",
			file:     "/data/events2/part-0.parquet",
			base:     "/data/events",
			wantErr:  io.ErrPathNotUnderBase,
			errWords: []string{"/data/events2/part-0.parquet", "/data/events"},
		},
		{
			name:    "a path outside the base",
			file:    "/other/table/part-0.parquet",
			base:    "/data/events",
			wantErr: io.ErrPathNotUnderBase,
		},
		{
			name:    "a different bucket",
			file:    "s3://other/events/part-0.parquet",
			base:    "s3://bucket/events",
			wantErr: io.ErrPathNotUnderBase,
		},
		{
			name:     "the base path itself",
			file:     "file:///data/events/",
			base:     "/data/events",
			wantErr:  io.ErrPathNotUnderBase,
			errWords: []string{"base path itself"},
		},
		{
			name:    "an empty base",
			file:    "/data/events/part-0.parquet",
			base:    "",
			wantErr: io.ErrInvalidPath,
		},
		{
			name:    "a base that is nothing but a scheme",
			file:    "s3://bucket/part-0.parquet",
			base:    "s3://",
			wantErr: io.ErrInvalidPath,
		},
		{
			name:    "an empty file path",
			file:    "",
			base:    "/data/events",
			wantErr: io.ErrInvalidPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := io.RelativizePath(tt.file, tt.base)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, got, "an error must not come with a path the caller might use")
				for _, word := range tt.errWords {
					assert.ErrorContains(t, err, word)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestRelativizePath_RoundTripsJoinPath pins the property every target depends on: what
// RelativizePath produces, JoinPath puts back where it came from.
func TestRelativizePath_RoundTripsJoinPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		base string
		file string
	}{
		{name: "local", base: "/data/events", file: "/data/events/data/part-0.parquet"},
		{name: "local base with a scheme", base: "file:///data/events", file: "file:///data/events/part-0.parquet"},
		{name: "s3", base: "s3://bucket/events", file: "s3://bucket/events/region=eu/part-0.parquet"},
		{name: "memory", base: "mem://table", file: "mem://table/data/part-0.parquet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rel, err := io.RelativizePath(tt.file, tt.base)
			require.NoError(t, err)
			assert.Equal(t, tt.file, io.JoinPath(tt.base, rel))
		})
	}
}

func TestTrimScheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantScheme string
		wantRest   string
	}{
		{name: "no scheme", path: "/data/events", wantScheme: "", wantRest: "/data/events"},
		{name: "file", path: "file:///data/events", wantScheme: "file://", wantRest: "/data/events"},
		{name: "s3", path: "s3://bucket/events", wantScheme: "s3://", wantRest: "bucket/events"},
		{name: "s3a", path: "s3a://bucket/events", wantScheme: "s3a://", wantRest: "bucket/events"},
		{name: "gs", path: "gs://bucket/events", wantScheme: "gs://", wantRest: "bucket/events"},
		{name: "mem", path: "mem://table", wantScheme: "mem://", wantRest: "table"},
		{name: "unrecognized scheme is left alone", path: "hdfs://ns/events", wantScheme: "", wantRest: "hdfs://ns/events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme, rest := io.TrimScheme(tt.path)
			assert.Equal(t, tt.wantScheme, scheme)
			assert.Equal(t, tt.wantRest, rest)
		})
	}
}
