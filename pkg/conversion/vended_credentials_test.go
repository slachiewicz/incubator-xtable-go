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

// Package conversion_test covers docs/improvement-plan.md T64's conversion-layer wiring: a table
// resolved from a credential-vending catalog carries its vended credentials through to the storage
// options the caller eventually constructs a Storage from, without those credentials ever passing
// through the serializable StorageConfig (see its own doc comment on why that must never happen).
package conversion_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestDatasetConfig_StorageOptionFuncs(t *testing.T) {
	t.Parallel()

	newCfg := func() *conversion.DatasetConfig {
		return &conversion.DatasetConfig{
			TargetFormats: []model.TableFormat{model.TableFormatIceberg},
			SourceCatalog: &conversion.SourceCatalogConfig{
				Catalog: catalog.Config{Type: catalog.CatalogTypeIcebergREST, DatabaseName: "analytics"},
				Table:   "events",
			},
		}
	}

	t.Run("a catalog vending nothing leaves behavior unchanged", func(t *testing.T) {
		t.Parallel()

		fake := &fakeConversionSource{table: &catalog.SourceTable{
			Name: "events", BasePath: "s3://lake/events", Format: model.TableFormatIceberg,
			// StorageCredentials left nil: the common case, and the one that must not regress.
		}}
		cfg := newCfg()
		cfg.Storage = &conversion.StorageConfig{Region: "eu-west-1"}

		require.NoError(t, conversion.ResolveSourceCatalog(context.Background(), cfg,
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil }))

		gotFromWiring := cfg.StorageOptionFuncs()
		gotFromStorageConfig := cfg.Storage.ToOptionFuncs()
		require.Len(t, gotFromWiring, len(gotFromStorageConfig),
			"no vended credentials means StorageOptionFuncs must add nothing beyond Storage.ToOptionFuncs")

		opts := &io.Options{}
		for _, fn := range gotFromWiring {
			fn(opts)
		}
		assert.Equal(t, "eu-west-1", opts.S3.Region)
		assert.Nil(t, opts.S3.Credentials)
	})

	t.Run("vended credentials are carried into S3 options", func(t *testing.T) {
		t.Parallel()

		expiry := time.Now().Add(time.Hour)
		fake := &fakeConversionSource{table: &catalog.SourceTable{
			Name: "events", BasePath: "s3://sfc-prod3-bucket/iceberg/db/events", Format: model.TableFormatIceberg,
			StorageCredentials: &catalog.StorageCredentials{
				AccessKeyID:     "AKIAVENDED",
				SecretAccessKey: "vended-secret",
				SessionToken:    "vended-token",
				Region:          "us-west-2",
				Expiration:      expiry,
			},
		}}
		cfg := newCfg()

		require.NoError(t, conversion.ResolveSourceCatalog(context.Background(), cfg,
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil }))

		opts := &io.Options{}
		for _, fn := range cfg.StorageOptionFuncs() {
			fn(opts)
		}

		require.NotNil(t, opts.S3.Credentials)
		assert.Equal(t, "AKIAVENDED", opts.S3.Credentials.AccessKeyID)
		assert.Equal(t, "vended-secret", opts.S3.Credentials.SecretAccessKey)
		assert.Equal(t, "vended-token", opts.S3.Credentials.SessionToken)
		assert.True(t, opts.S3.Credentials.Expiration.Equal(expiry))
		assert.Equal(t, "us-west-2", opts.S3.Region)
	})

	t.Run("the vended region wins over a configured region", func(t *testing.T) {
		t.Parallel()

		// This is the PermanentRedirect case: a user-configured region for a different bucket must
		// not survive over the region the vended credentials are actually scoped to.
		fake := &fakeConversionSource{table: &catalog.SourceTable{
			Name: "events", BasePath: "s3://sfc-prod3-bucket/iceberg/db/events", Format: model.TableFormatIceberg,
			StorageCredentials: &catalog.StorageCredentials{
				AccessKeyID: "AKIAVENDED", SecretAccessKey: "vended-secret", Region: "us-west-2",
			},
		}}
		cfg := newCfg()
		cfg.Storage = &conversion.StorageConfig{Region: "us-east-1"}

		require.NoError(t, conversion.ResolveSourceCatalog(context.Background(), cfg,
			func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil }))

		opts := &io.Options{}
		for _, fn := range cfg.StorageOptionFuncs() {
			fn(opts)
		}
		assert.Equal(t, "us-west-2", opts.S3.Region)
	})

	t.Run("no source catalog at all behaves exactly like Storage.ToOptionFuncs", func(t *testing.T) {
		t.Parallel()

		cfg := &conversion.DatasetConfig{
			TableBasePath: "s3://lake/events",
			Storage:       &conversion.StorageConfig{Region: "ap-southeast-2"},
		}

		gotFromWiring := cfg.StorageOptionFuncs()
		gotFromStorageConfig := cfg.Storage.ToOptionFuncs()
		require.Len(t, gotFromWiring, len(gotFromStorageConfig))

		opts := &io.Options{}
		for _, fn := range gotFromWiring {
			fn(opts)
		}
		assert.Equal(t, "ap-southeast-2", opts.S3.Region)
		assert.Nil(t, opts.S3.Credentials)
	})
}

func TestDiscoverDatasets_CarriesVendedCredentials(t *testing.T) {
	t.Parallel()

	fake := &fakeConversionSource{
		listed: []string{"events"},
		tables: map[string]*catalog.SourceTable{
			"events": {
				Name: "events", BasePath: "s3://sfc-prod3-bucket/iceberg/db/events",
				Format:     model.TableFormatIceberg,
				Properties: map[string]string{catalog.PropTargetFormats: "DELTA"},
				StorageCredentials: &catalog.StorageCredentials{
					AccessKeyID: "AKIADISCOVERED", SecretAccessKey: "discovered-secret", Region: "us-west-2",
				},
			},
		},
	}

	cfg := &catalog.Config{Type: catalog.CatalogTypeIcebergREST, DatabaseName: "analytics"}
	datasets, err := conversion.DiscoverDatasets(context.Background(), cfg,
		func(context.Context, *catalog.Config) (catalog.ConversionSource, error) { return fake, nil })
	require.NoError(t, err)
	require.Len(t, datasets, 1)

	opts := &io.Options{}
	for _, fn := range datasets[0].StorageOptionFuncs() {
		fn(opts)
	}
	require.NotNil(t, opts.S3.Credentials)
	assert.Equal(t, "AKIADISCOVERED", opts.S3.Credentials.AccessKeyID)
	assert.Equal(t, "us-west-2", opts.S3.Region)
}
