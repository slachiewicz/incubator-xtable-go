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

package io

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// AzureStorage provides a zero-JVM native Azure Blob Storage implementation, reached through
// abfss://, abfs://, wasbs:// or wasb:// paths.
type AzureStorage struct {
	client *azblob.Client
	// host and scheme are kept so List can rebuild the absolute path a caller passed in. The
	// account is deliberately not kept: it is needed to build the client and never afterwards,
	// because an abfss:// path names the account through host, not as a separate component.
	host   string // the original URI host, e.g. "acct.dfs.core.windows.net" or "onelake.dfs.fabric.microsoft.com"
	scheme string // the original URI scheme, e.g. "abfss"
}

var _ Storage = (*AzureStorage)(nil)

// AzureOptions allows configuring the service endpoint, account, and credentials for Azure Blob
// Storage access. The zero value resolves everything from the URI and the environment.
type AzureOptions struct {
	// Endpoint overrides the derived blob service URL. Required for Azurite and for any
	// deployment whose blob host is not derivable from the abfss:// host.
	Endpoint string
	// AccountName overrides the account parsed from the URI host. Azurite needs it: its
	// service URL carries the account in the path rather than the host.
	AccountName string
	// AccountKey authenticates with a shared key. Prefer the AZURE_STORAGE_KEY environment
	// variable; this field exists for tests and for callers that already hold the secret.
	AccountKey string
	// SASToken authenticates with a shared access signature, with or without a leading "?".
	SASToken string
	// Anonymous selects unauthenticated access, for a public container.
	Anonymous bool
	// CustomHTTPClient injects a transport. Tests use it; production should not need it.
	CustomHTTPClient *http.Client
}

// NewAzureStorage creates a new AzureStorage instance for the account addressed by uri. The URI is
// required at construction, unlike S3 where the region comes from configuration, because the
// account host is part of the address itself.
func NewAzureStorage(ctx context.Context, uri string, optFns ...func(*AzureOptions)) (*AzureStorage, error) {
	opts := AzureOptions{}
	for _, fn := range optFns {
		fn(&opts)
	}

	_, _, host, scheme, err := ParseAzureURI(uri)
	if err != nil {
		return nil, err
	}

	// The account is the first dot-separated label of the host, e.g. "acct" for
	// acct.dfs.core.windows.net. OneLake needs no special case: its host already starts with
	// "onelake.", so the same rule yields the literal account name "onelake" — the workspace and
	// item live in the container instead of the account.
	account := opts.AccountName
	if account == "" {
		account, _, _ = strings.Cut(host, ".")
	}

	// The blob (service) endpoint differs from the abfss:// host, which names the Data Lake
	// Storage (dfs) endpoint. Swapping the first ".dfs." for ".blob." derives it for the common
	// case; a host already reading ".blob." is unaffected, since there is nothing to replace.
	// OneLake documents the same pair — onelake.dfs.fabric.microsoft.com and
	// onelake.blob.fabric.microsoft.com, with the blob endpoint carrying the same compatibility
	// as the ADLS one — so the swap is the documented mapping rather than a guess, though no
	// request from this package has yet reached a Fabric workspace.
	//
	// Endpoint overrides it, and there are three shapes that need the override: a regional
	// endpoint (<region>-onelake.dfs.fabric.microsoft.com, which OneLake recommends over the
	// global one so data does not cross a region boundary during endpoint resolution), a
	// workspace private-link FQDN, and the api.onelake.fabric.microsoft.com form, which carries
	// neither ".dfs." nor ".blob." and so survives the swap untouched.
	serviceURL := opts.Endpoint
	if serviceURL == "" {
		serviceURL = "https://" + strings.Replace(host, ".dfs.", ".blob.", 1)
	}

	var clientOptions *azblob.ClientOptions
	if opts.CustomHTTPClient != nil {
		clientOptions = &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: opts.CustomHTTPClient}}
	}

	sasToken := opts.SASToken
	if sasToken == "" {
		sasToken = os.Getenv("AZURE_STORAGE_SAS_TOKEN")
	}
	accountKey := opts.AccountKey
	if accountKey == "" {
		accountKey = os.Getenv("AZURE_STORAGE_KEY")
	}

	var client *azblob.Client
	switch {
	case sasToken != "":
		sasURL := serviceURL + "?" + strings.TrimPrefix(sasToken, "?")
		client, err = azblob.NewClientWithNoCredential(sasURL, clientOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client with SAS token: %w", err)
		}
	case accountKey != "":
		cred, credErr := azblob.NewSharedKeyCredential(account, accountKey)
		if credErr != nil {
			return nil, fmt.Errorf("failed to create Azure shared key credential: %w", credErr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, clientOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client with shared key credential: %w", err)
		}
	case opts.Anonymous:
		client, err = azblob.NewClientWithNoCredential(serviceURL, clientOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create anonymous Azure client: %w", err)
		}
	default:
		// Covers workload identity, managed identity, an environment service principal, and the
		// Azure CLI, in that order, via the standard credential chain.
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return nil, fmt.Errorf("failed to create default Azure credential: %w", credErr)
		}
		client, err = azblob.NewClient(serviceURL, cred, clientOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure client: %w", err)
		}
	}

	return &AzureStorage{
		client: client,
		host:   host,
		scheme: scheme,
	}, nil
}

// NewAzureStorageWithClient initializes AzureStorage with an existing Azure Blob client, for
// injecting a client that already points at a test server (Azurite) or a preconfigured credential.
func NewAzureStorageWithClient(client *azblob.Client, host, scheme string) *AzureStorage {
	return &AzureStorage{client: client, host: host, scheme: scheme}
}

// Read fetches a blob from Azure Blob Storage.
func (s *AzureStorage) Read(ctx context.Context, path string) ([]byte, error) {
	container, blob, _, _, err := ParseAzureURI(path)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.DownloadStream(ctx, container, blob, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return nil, fmt.Errorf("failed to get azure blob %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	return io.ReadAll(resp.Body)
}

// Write uploads data to Azure Blob Storage.
func (s *AzureStorage) Write(ctx context.Context, path string, data []byte) error {
	container, blob, _, _, err := ParseAzureURI(path)
	if err != nil {
		return err
	}

	_, err = s.client.UploadBuffer(ctx, container, blob, data, nil)
	if err != nil {
		return fmt.Errorf("failed to put azure blob %s: %w", path, err)
	}
	return nil
}

// List lists all Azure blobs matching the prefix.
func (s *AzureStorage) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	container, blobPath, _, _, err := ParseAzureURI(prefix)
	if err != nil {
		return nil, err
	}

	var results []FileInfo
	pager := s.client.NewListBlobsFlatPager(container, &azblob.ListBlobsFlatOptions{
		Prefix: &blobPath,
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list azure blobs with prefix %s: %w", prefix, err)
		}

		for _, item := range page.Segment.BlobItems {
			var name string
			if item.Name != nil {
				name = *item.Name
			}
			var size int64
			var modTime time.Time
			if item.Properties != nil {
				if item.Properties.ContentLength != nil {
					size = *item.Properties.ContentLength
				}
				if item.Properties.LastModified != nil {
					modTime = *item.Properties.LastModified
				}
			}
			fullPath := fmt.Sprintf("%s://%s@%s/%s", s.scheme, container, s.host, name)
			results = append(results, FileInfo{
				Path:    fullPath,
				Size:    size,
				ModTime: modTime,
				IsDir:   strings.HasSuffix(name, "/"),
			})
		}
	}

	return results, nil
}

// Exists checks if a blob exists in Azure Blob Storage.
func (s *AzureStorage) Exists(ctx context.Context, path string) (bool, error) {
	container, blob, _, _, err := ParseAzureURI(path)
	if err != nil {
		return false, err
	}

	_, err = s.client.ServiceClient().NewContainerClient(container).NewBlobClient(blob).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Delete removes a blob from Azure Blob Storage.
func (s *AzureStorage) Delete(ctx context.Context, path string) error {
	container, blob, _, _, err := ParseAzureURI(path)
	if err != nil {
		return err
	}

	_, err = s.client.DeleteBlob(ctx, container, blob, nil)
	if err != nil {
		return fmt.Errorf("failed to delete azure blob %s: %w", path, err)
	}
	return nil
}

// Close is a no-op for AzureStorage.
func (s *AzureStorage) Close() error {
	return nil
}

// ParseAzureURI extracts the container, blob path, account host and scheme from an abfss://,
// abfs://, wasbs:// or wasb:// URI, e.g. abfss://container@acct.dfs.core.windows.net/path.
func ParseAzureURI(uri string) (container, blobPath, host, scheme string, err error) {
	schemes := []string{"abfss://", "abfs://", "wasbs://", "wasb://"}
	var matched string
	for _, s := range schemes {
		if strings.HasPrefix(uri, s) {
			matched = s
			break
		}
	}
	if matched == "" {
		return "", "", "", "", fmt.Errorf("%w: path must start with abfss://, abfs://, wasbs:// or wasb://", ErrInvalidPath)
	}
	scheme = strings.TrimSuffix(matched, "://")
	cleaned := strings.TrimPrefix(uri, matched)

	atIdx := strings.Index(cleaned, "@")
	if atIdx <= 0 {
		return "", "", "", "", fmt.Errorf("%w: missing container in %s", ErrInvalidPath, uri)
	}
	container = cleaned[:atIdx]

	rest := cleaned[atIdx+1:]
	parts := strings.SplitN(rest, "/", 2)
	host = parts[0]
	if host == "" {
		return "", "", "", "", fmt.Errorf("%w: missing account host in %s", ErrInvalidPath, uri)
	}
	if len(parts) > 1 {
		blobPath = parts[1]
	}

	return container, blobPath, host, scheme, nil
}
