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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

// listBlobsFlatResponse is a hand-written but field-faithful ListBlobs response, covering the
// shapes that distinguish an ADLS Gen2 directory from a Blob-Storage-convention one and from an
// ordinary data file. The element and attribute names mirror the SDK's own
// internal/generated.BlobItem / BlobProperties xml tags (ResourceType, Metadata, Content-Length,
// Last-Modified), not a guess at what "looks like" the real service.
const listBlobsFlatResponse = `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="http://127.0.0.1/devstoreaccount1" ContainerName="lakehouse-e2e">
  <Prefix>tables/t1/</Prefix>
  <Blobs>
    <Blob>
      <Name>tables/t1/region=EU</Name>
      <Properties>
        <Last-Modified>Mon, 01 Jan 2024 00:00:00 GMT</Last-Modified>
        <Etag>0x8D1234567890ABC</Etag>
        <Content-Length>0</Content-Length>
        <Content-Type>application/octet-stream</Content-Type>
        <BlobType>BlockBlob</BlobType>
        <ResourceType>directory</ResourceType>
      </Properties>
    </Blob>
    <Blob>
      <Name>tables/t1/region=US</Name>
      <Properties>
        <Last-Modified>Mon, 01 Jan 2024 00:00:00 GMT</Last-Modified>
        <Etag>0x8D1234567890ABD</Etag>
        <Content-Length>0</Content-Length>
        <Content-Type>application/octet-stream</Content-Type>
        <BlobType>BlockBlob</BlobType>
      </Properties>
      <Metadata>
        <hdi_isfolder>true</hdi_isfolder>
      </Metadata>
    </Blob>
    <Blob>
      <Name>tables/t1/legacy/</Name>
      <Properties>
        <Last-Modified>Mon, 01 Jan 2024 00:00:00 GMT</Last-Modified>
        <Etag>0x8D1234567890ABE</Etag>
        <Content-Length>0</Content-Length>
        <Content-Type>application/octet-stream</Content-Type>
        <BlobType>BlockBlob</BlobType>
      </Properties>
    </Blob>
    <Blob>
      <Name>tables/t1/region=EU/data-001.parquet</Name>
      <Properties>
        <Last-Modified>Mon, 01 Jan 2024 00:00:00 GMT</Last-Modified>
        <Etag>0x8D1234567890ABF</Etag>
        <Content-Length>1024</Content-Length>
        <Content-Type>application/octet-stream</Content-Type>
        <BlobType>BlockBlob</BlobType>
      </Properties>
    </Blob>
    <Blob>
      <Name>tables/t1/noprops</Name>
    </Blob>
  </Blobs>
  <NextMarker />
</EnumerationResults>
`

// TestAzureStorage_List_ADLSGen2Directories exercises the fix for List's IsDir determination
// against a hand-written ListBlobs XML response served over httptest, rather than against
// Azurite: Azurite has no hierarchical namespace, so it can never produce a ResourceType or
// hdi_isfolder blob and cannot catch a regression here. The five blobs cover: a real ADLS Gen2
// directory object (ResourceType=directory, no trailing slash), a directory identified only by
// the hdi_isfolder metadata key, a Blob-Storage-convention "directory" (trailing slash, neither
// ADLS Gen2 signal), an ordinary data file, and a blob with no Properties element at all (nil
// Properties and nil Metadata — the XML schema marks Properties "REQUIRED" but xml.Unmarshal does
// not enforce that, so isDirectory's nil guards need a real exercise, not just an inspection).
//
// One nil case in isDirectory is not reachable from here and is not attempted: a nil *string
// inside Metadata. The SDK's own additionalProperties.UnmarshalXML always wraps the decoded value
// with to.Ptr, never nil, and that type is unexported, so no XML body fed through the public
// azblob.Client can produce it; the check is defensive, not currently exercisable black-box.
func TestAzureStorage_List_ADLSGen2Directories(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(listBlobsFlatResponse))
	}))
	defer server.Close()

	client, err := azblob.NewClientWithNoCredential(server.URL, nil)
	require.NoError(t, err)

	storage := io.NewAzureStorageWithClient(client, "acct.dfs.core.windows.net", "abfss")

	infos, err := storage.List(context.Background(), "abfss://lakehouse-e2e@acct.dfs.core.windows.net/tables/t1/")
	require.NoError(t, err)
	require.Len(t, infos, 5)

	byName := make(map[string]io.FileInfo, len(infos))
	for _, info := range infos {
		byName[info.Path] = info
	}

	tests := []struct {
		name     string
		path     string
		wantDir  bool
		wantSize int64
	}{
		{
			name:    "ADLS Gen2 directory via ResourceType",
			path:    "abfss://lakehouse-e2e@acct.dfs.core.windows.net/tables/t1/region=EU",
			wantDir: true,
		},
		{
			name:    "ADLS Gen2 directory via hdi_isfolder metadata",
			path:    "abfss://lakehouse-e2e@acct.dfs.core.windows.net/tables/t1/region=US",
			wantDir: true,
		},
		{
			name:    "Blob-Storage-convention directory (trailing slash)",
			path:    "abfss://lakehouse-e2e@acct.dfs.core.windows.net/tables/t1/legacy/",
			wantDir: true,
		},
		{
			name:     "ordinary data file",
			path:     "abfss://lakehouse-e2e@acct.dfs.core.windows.net/tables/t1/region=EU/data-001.parquet",
			wantDir:  false,
			wantSize: 1024,
		},
		{
			name:    "nil-safe on absent Properties and Metadata",
			path:    "abfss://lakehouse-e2e@acct.dfs.core.windows.net/tables/t1/noprops",
			wantDir: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			info, ok := byName[tt.path]
			require.True(t, ok, "expected an entry for %q, got %+v", tt.path, infos)
			assert.Equal(t, tt.wantDir, info.IsDir)
			assert.Equal(t, tt.wantSize, info.Size)
		})
	}
}
