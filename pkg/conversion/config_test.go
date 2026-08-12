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

package conversion_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/io"
)

func TestStorageConfig_ToS3OptionFuncs_NilConfig(t *testing.T) {
	t.Parallel()

	var config *conversion.StorageConfig
	optFns := config.ToS3OptionFuncs()

	assert.Nil(t, optFns, "nil config should produce nil option functions")
}

func TestStorageConfig_ToS3OptionFuncs_EmptyConfig(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{}
	optFns := config.ToS3OptionFuncs()

	assert.NotNil(t, optFns, "empty config should produce non-nil slice")
	assert.Empty(t, optFns, "empty config should produce empty slice")
}

func TestStorageConfig_ToS3OptionFuncs_RegionOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region: "us-west-2",
	}

	optFns := config.ToS3OptionFuncs()

	require.Len(t, optFns, 1, "region-only config should produce one option function")

	opts := &io.S3Options{}
	optFns[0](opts)

	assert.Equal(t, "us-west-2", opts.Region)
	assert.Empty(t, opts.Endpoint)
	assert.False(t, opts.UsePathStyle)
}

func TestStorageConfig_ToS3OptionFuncs_EndpointOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Endpoint: "http://localhost:9000",
	}

	optFns := config.ToS3OptionFuncs()

	require.Len(t, optFns, 1, "endpoint-only config should produce one option function")

	opts := &io.S3Options{}
	optFns[0](opts)

	assert.Empty(t, opts.Region)
	assert.Equal(t, "http://localhost:9000", opts.Endpoint)
	assert.False(t, opts.UsePathStyle)
}

func TestStorageConfig_ToS3OptionFuncs_UsePathStyleOnly(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		UsePathStyle: true,
	}

	optFns := config.ToS3OptionFuncs()

	require.Len(t, optFns, 1, "path-style-only config should produce one option function")

	opts := &io.S3Options{}
	optFns[0](opts)

	assert.Empty(t, opts.Region)
	assert.Empty(t, opts.Endpoint)
	assert.True(t, opts.UsePathStyle)
}

func TestStorageConfig_ToS3OptionFuncs_AllOptions(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region:       "eu-west-1",
		Endpoint:     "https://minio.example.com",
		UsePathStyle: true,
	}

	optFns := config.ToS3OptionFuncs()

	require.Len(t, optFns, 3, "all options should produce three option functions")

	opts := &io.S3Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "eu-west-1", opts.Region)
	assert.Equal(t, "https://minio.example.com", opts.Endpoint)
	assert.True(t, opts.UsePathStyle)
}

func TestStorageConfig_ToS3OptionFuncs_PartialOptions(t *testing.T) {
	t.Parallel()

	config := &conversion.StorageConfig{
		Region:   "ap-southeast-2",
		Endpoint: "http://s3-gateway.local",
	}

	optFns := config.ToS3OptionFuncs()

	require.Len(t, optFns, 2, "partial options should produce two option functions")

	opts := &io.S3Options{}
	for _, fn := range optFns {
		fn(opts)
	}

	assert.Equal(t, "ap-southeast-2", opts.Region)
	assert.Equal(t, "http://s3-gateway.local", opts.Endpoint)
	assert.False(t, opts.UsePathStyle)
}

func TestDatasetConfig_StorageField(t *testing.T) {
	t.Parallel()

	config := &conversion.DatasetConfig{
		Storage: &conversion.StorageConfig{
			Region:   "us-east-1",
			Endpoint: "http://localhost:9000",
		},
	}

	require.NotNil(t, config.Storage, "storage field should be set")
	optFns := config.Storage.ToS3OptionFuncs()

	require.Len(t, optFns, 2, "dataset storage should produce option functions")
}

func TestDatasetConfig_NilStorage(t *testing.T) {
	t.Parallel()

	config := &conversion.DatasetConfig{}

	require.Nil(t, config.Storage, "storage field should be nil when not set")
}
