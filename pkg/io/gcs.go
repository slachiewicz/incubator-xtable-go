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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	storagev1 "google.golang.org/api/storage/v1"
	htransport "google.golang.org/api/transport/http"
)

// These mirror the unexported basePathTemplate/mtlsBasePath constants that storagev1.NewService
// prepends to every call. They are duplicated here, rather than left for NewService to apply on
// its own, because building the *http.Client ourselves (see newAuthedHTTPClient) is the only way
// to retain a handle for Close to release: storagev1.Service keeps its client unexported. A
// storagev1 upgrade that changes these is unlikely — they are the public GCS JSON API endpoints —
// but would need a matching update here.
const (
	gcsBasePathTemplate = "https://storage.UNIVERSE_DOMAIN/storage/v1/"
	gcsMTLSBasePath     = "https://storage.mtls.googleapis.com/storage/v1/"
)

// gcsScopes mirrors the scopes storagev1.NewService requests by default. A service-account
// credential (the CredentialsFile option) needs an explicit scope list to mint a usable token;
// Application Default Credentials and anonymous access ignore it.
var gcsScopes = []string{
	"https://www.googleapis.com/auth/cloud-platform",
	"https://www.googleapis.com/auth/cloud-platform.read-only",
	"https://www.googleapis.com/auth/devstorage.full_control",
	"https://www.googleapis.com/auth/devstorage.read_only",
	"https://www.googleapis.com/auth/devstorage.read_write",
}

// GCSStorage provides a zero-JVM native Google Cloud Storage implementation, reached through
// gs:// paths. It talks to the GCS JSON API (google.golang.org/api/storage/v1) directly over
// net/http rather than through cloud.google.com/go/storage: that package links gRPC, its
// OpenTelemetry instrumentation, genproto and s2a-go unconditionally to support its gRPC
// transport, even though this backend only ever needs the JSON transport. The JSON client is a
// thin, hand-maintained generated wrapper over the same REST API and avoids that cost.
type GCSStorage struct {
	svc *storagev1.Service
	// httpClient is the transport GCSStorage built for svc. storagev1.Service keeps its own
	// client unexported, so this is the only handle Close has to release pooled connections.
	httpClient *http.Client
}

var _ Storage = (*GCSStorage)(nil)

// GCSOptions allows configuring the service endpoint and credentials for Google Cloud Storage
// access. The zero value resolves credentials from the Application Default Credentials chain,
// which covers, in order: the GOOGLE_APPLICATION_CREDENTIALS environment variable, gcloud's
// user credentials (from `gcloud auth application-default login`), and the GCE/GKE metadata
// server's attached service account under workload identity. No literal credential ever lives
// in this struct: a config file gets committed, logged and POSTed to the REST service, so only a
// path to a credentials file is accepted, never its contents.
type GCSOptions struct {
	// Endpoint overrides the derived storage service URL. Required for a fake or emulator, e.g.
	// fake-gcs-server. This is the only supported way to point at one: unlike
	// cloud.google.com/go/storage's NewClient, NewGCSStorage does not read STORAGE_EMULATOR_HOST
	// itself, so that variable being set in the environment has no effect here.
	Endpoint string
	// AnonymousAccess selects unauthenticated access, for a public bucket.
	AnonymousAccess bool
	// CredentialsFile names a service-account JSON file's path, never the JSON itself. Prefer
	// GOOGLE_APPLICATION_CREDENTIALS, the well-known Application Default Credentials variable,
	// over this field; it exists for tests and for a process that needs to pick a different
	// file than what's ambient in its environment.
	CredentialsFile string
}

// newAuthedHTTPClient builds the *http.Client storagev1.NewService would otherwise build for
// itself internally, so that GCSStorage keeps a handle to close. Passing the result back in via
// option.WithHTTPClient makes storagev1.NewService use it as-is (see
// google.golang.org/api/transport/http.NewClient: a supplied HTTPClient short-circuits its own
// auth resolution), so credential precedence is decided once, here.
func newAuthedHTTPClient(ctx context.Context, opts []option.ClientOption) (*http.Client, error) {
	opts = append([]option.ClientOption{
		internaloption.WithDefaultScopes(gcsScopes...),
	}, opts...)
	opts = append(opts,
		// WithDefaultEndpointTemplate plus WithDefaultUniverseDomain, not the deprecated
		// WithDefaultEndpoint: the template's "UNIVERSE_DOMAIN" placeholder resolves against
		// the universe domain to "https://storage.googleapis.com/storage/v1/", without the
		// deprecated call.
		internaloption.WithDefaultEndpointTemplate(gcsBasePathTemplate),
		internaloption.WithDefaultUniverseDomain("googleapis.com"),
		internaloption.WithDefaultMTLSEndpoint(gcsMTLSBasePath),
		internaloption.EnableNewAuthLibrary(),
	)
	client, _, err := htransport.NewClient(ctx, opts...)
	return client, err
}

// NewGCSStorage creates a new GCSStorage instance. The bucket is not part of construction, unlike
// Azure where the account host is part of the address: a GCS client is bucket-agnostic, and the
// bucket is resolved per call from the gs:// path, the same as S3.
func NewGCSStorage(ctx context.Context, optFns ...func(*GCSOptions)) (*GCSStorage, error) {
	opts := GCSOptions{}
	for _, fn := range optFns {
		fn(&opts)
	}

	var clientOpts []option.ClientOption
	if opts.Endpoint != "" {
		clientOpts = append(clientOpts, option.WithEndpoint(opts.Endpoint))
	}

	switch {
	case opts.CredentialsFile != "":
		// WithAuthCredentialsFile, not the deprecated WithCredentialsFile: naming ServiceAccount
		// explicitly means a file of an unexpected credential type is rejected rather than
		// loaded, which is exactly the risk WithCredentialsFile's deprecation warns about.
		clientOpts = append(clientOpts, option.WithAuthCredentialsFile(option.ServiceAccount, opts.CredentialsFile))
	case opts.AnonymousAccess:
		clientOpts = append(clientOpts, option.WithoutAuthentication())
	default:
		// Covers GOOGLE_APPLICATION_CREDENTIALS, gcloud user credentials, and workload identity
		// on GCE/GKE, via the standard Application Default Credentials chain.
	}

	httpClient, err := newAuthedHTTPClient(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	svcOpts := append(append([]option.ClientOption{}, clientOpts...), option.WithHTTPClient(httpClient))
	svc, err := storagev1.NewService(ctx, svcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &GCSStorage{svc: svc, httpClient: httpClient}, nil
}

// NewGCSStorageWithClient initializes GCSStorage with an existing storagev1.Service, for injecting
// a client that already points at a test server (fake-gcs-server, or an httptest stand-in) or a
// preconfigured credential.
func NewGCSStorageWithClient(svc *storagev1.Service) *GCSStorage {
	return &GCSStorage{svc: svc}
}

// isGCSNotFound reports whether err is the JSON API's 404 response for an object that doesn't
// exist, google.golang.org/api's equivalent of cloud.google.com/go/storage.ErrObjectNotExist.
func isGCSNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound
}

// Read fetches an object from Google Cloud Storage.
func (s *GCSStorage) Read(ctx context.Context, path string) ([]byte, error) {
	bucket, object, err := ParseGCSURI(path)
	if err != nil {
		return nil, err
	}

	resp, err := s.svc.Objects.Get(bucket, object).Context(ctx).Download()
	if err != nil {
		if isGCSNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return nil, fmt.Errorf("failed to get gcs object %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	return io.ReadAll(resp.Body)
}

// Write uploads data to Google Cloud Storage.
func (s *GCSStorage) Write(ctx context.Context, path string, data []byte) error {
	bucket, object, err := ParseGCSURI(path)
	if err != nil {
		return err
	}

	_, err = s.svc.Objects.Insert(bucket, &storagev1.Object{Name: object}).
		Media(bytes.NewReader(data)).
		Context(ctx).
		Do()
	if err != nil {
		return fmt.Errorf("failed to put gcs object %s: %w", path, err)
	}
	return nil
}

// List lists all Google Cloud Storage objects matching the prefix.
func (s *GCSStorage) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	bucket, objectPrefix, err := ParseGCSURI(prefix)
	if err != nil {
		return nil, err
	}

	var results []FileInfo
	pageToken := ""
	for {
		call := s.svc.Objects.List(bucket).Prefix(objectPrefix).Context(ctx)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}

		resp, err := call.Do()
		if err != nil {
			return nil, fmt.Errorf("failed to list gcs objects with prefix %s: %w", prefix, err)
		}

		for _, item := range resp.Items {
			// Updated is RFC 3339; the JSON API never omits it on a real object, but an empty
			// or malformed value degrades to the zero time rather than failing the whole list.
			modTime, _ := time.Parse(time.RFC3339, item.Updated)
			fullPath := fmt.Sprintf("gs://%s/%s", bucket, item.Name)
			results = append(results, FileInfo{
				Path:    fullPath,
				Size:    gcsObjectSize(item.Size),
				ModTime: modTime,
				// GCS has no real directories, only object names that happen to contain "/": every
				// listed result names a real object, so IsDir follows the same trailing-slash
				// convention as S3. There is no ADLS Gen2-style equivalent to azure.go's isDirectory
				// helper, because GCS never materializes a directory as an object the way ADLS Gen2
				// does — a "folder" is purely a naming convention clients agree on, never a signal
				// the service itself returns.
				IsDir: strings.HasSuffix(item.Name, "/"),
			})
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	return results, nil
}

// gcsObjectSize converts the JSON API's uint64 object size to FileInfo's int64, clamping rather
// than wrapping on the one object in the universe bigger than math.MaxInt64 bytes (8 exbibytes),
// which is also well past GCS's own per-object size limit.
func gcsObjectSize(size uint64) int64 {
	if size > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(size)
}

// Exists checks if an object exists in Google Cloud Storage.
func (s *GCSStorage) Exists(ctx context.Context, path string) (bool, error) {
	bucket, object, err := ParseGCSURI(path)
	if err != nil {
		return false, err
	}

	_, err = s.svc.Objects.Get(bucket, object).Context(ctx).Do()
	if err != nil {
		if isGCSNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Delete removes an object from Google Cloud Storage.
func (s *GCSStorage) Delete(ctx context.Context, path string) error {
	bucket, object, err := ParseGCSURI(path)
	if err != nil {
		return err
	}

	if err := s.svc.Objects.Delete(bucket, object).Context(ctx).Do(); err != nil {
		return fmt.Errorf("failed to delete gcs object %s: %w", path, err)
	}
	return nil
}

// Close releases the pooled connections GCSStorage's transport holds. It is a no-op when
// GCSStorage was built with NewGCSStorageWithClient, whose caller owns the underlying client and
// its lifetime.
func (s *GCSStorage) Close() error {
	if s.httpClient != nil {
		s.httpClient.CloseIdleConnections()
	}
	return nil
}

// ParseGCSURI extracts the bucket and object name from a gs:// URI.
func ParseGCSURI(uri string) (bucket, object string, err error) {
	if !strings.HasPrefix(uri, "gs://") {
		return "", "", fmt.Errorf("%w: path must start with gs://", ErrInvalidPath)
	}
	cleaned := strings.TrimPrefix(uri, "gs://")

	parts := strings.SplitN(cleaned, "/", 2)
	if parts[0] == "" {
		return "", "", fmt.Errorf("%w: missing bucket name in %s", ErrInvalidPath, uri)
	}

	bucket = parts[0]
	if len(parts) > 1 {
		object = parts[1]
	}
	return bucket, object, nil
}
