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

package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// emptyPayloadHashHex is the hex-encoded SHA-256 hash of an empty byte string, the well-known value
// SigV4 requires for a request carrying no body (every GET this package issues).
const emptyPayloadHashHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// sigv4Transport is an http.RoundTripper that SigV4-signs every request for one AWS service and
// region, refreshing credentials from credentials on each request rather than caching them itself
// (they expire and rotate -- IMDS and SSO both refresh -- and aws.CredentialsCache, which the
// constructors below wrap credentials in, already handles that caching correctly).
type sigv4Transport struct {
	base        http.RoundTripper
	signer      *v4.Signer
	credentials aws.CredentialsProvider
	region      string
	signingName string
}

// RoundTrip implements http.RoundTripper. It never mutates req, as required by the RoundTripper
// contract: it clones the request, exactly as entraTransport does, and signs the clone.
//
// req.Clone does not deep-copy Body -- it is the same rule net/http documents for Request.Clone --
// so when payloadHashForRequest below buffers a POST/PUT body it necessarily drains req's original
// Body stream too, not just the clone's. That is unavoidable for any transport that must read a
// body to hash it and still send it on; it is exactly what net/http's own transport does with
// single-use request bodies, and it does not touch any of req's fields, only the shared io.Reader's
// position.
func (t *sigv4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())

	payloadHash, err := payloadHashForRequest(clone)
	if err != nil {
		return nil, err
	}
	// SignHTTP signs whatever headers are already on the request; it does not add this one itself
	// (that is only PresignHTTP's business, and only for query-string signing). Setting it before
	// signing makes the payload hash part of SignedHeaders and gives the server the same signal S3
	// and Glue's own SDK requests carry, so a receiving service can verify the body against the
	// header instead of only against the signature.
	clone.Header.Set("X-Amz-Content-Sha256", payloadHash)

	creds, err := t.credentials.Retrieve(clone.Context())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve AWS credentials for SigV4 signing (region=%s, service=%s): %w", t.region, t.signingName, err)
	}

	if err := t.signer.SignHTTP(clone.Context(), creds, clone, payloadHash, t.signingName, t.region, time.Now()); err != nil {
		return nil, fmt.Errorf("failed to SigV4-sign request (region=%s, service=%s): %w", t.region, t.signingName, err)
	}

	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

// payloadHashForRequest returns the hex-encoded SHA-256 SigV4 needs for req's body. A nil or
// http.NoBody body (every GET) uses the well-known empty-payload hash. Anything else is buffered
// into memory so it can be hashed, then req.Body (and GetBody, and ContentLength) are reset to a
// fresh reader over that buffer so the caller -- here, the base transport that sends the signed
// clone -- can still read the body after this function has already consumed it once. Skipping this
// reset in favor of an "unsigned payload" shortcut is not valid for Glue or S3 Tables: both require
// a real payload hash, not the S3-object-API-only UNSIGNED-PAYLOAD sentinel.
func payloadHashForRequest(req *http.Request) (string, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return emptyPayloadHashHex, nil
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return "", fmt.Errorf("failed to buffer request body for SigV4 signing: %w", err)
	}
	_ = req.Body.Close()

	req.Body = io.NopCloser(bytes.NewReader(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	req.ContentLength = int64(len(body))

	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// newSigV4HTTPClient builds an HTTP client whose every request is SigV4-signed for signingName in
// region, with credentials from the AWS SDK's standard chain (environment, shared config, SSO,
// container credentials, IMDS) -- the same chain pkg/io/s3.go relies on, via
// awsconfig.LoadDefaultConfig. region may be empty, in which case the SDK's own region resolution
// (AWS_REGION, AWS_DEFAULT_REGION, the shared config file) is used instead; if that also resolves
// nothing, this fails rather than signing with an empty region, which SigV4 does not accept.
func newSigV4HTTPClient(timeout time.Duration, region, signingName string) (*http.Client, error) {
	var configFns []func(*awsconfig.LoadOptions) error
	if region != "" {
		configFns = append(configFns, awsconfig.WithRegion(region))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), configFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS default configuration for SigV4 signing: %w", err)
	}

	resolvedRegion := region
	if resolvedRegion == "" {
		resolvedRegion = cfg.Region
	}
	if resolvedRegion == "" {
		return nil, fmt.Errorf("SigV4 signing requires an AWS region: set catalog property %q, or AWS_REGION/AWS_DEFAULT_REGION, or use a warehouse URI shaped like <service>.<region>.amazonaws.com", PropCatalogRegion)
	}

	return NewSigV4HTTPClientWithCredentials(cfg.Credentials, timeout, resolvedRegion, signingName), nil
}

// NewSigV4HTTPClientWithCredentials builds the same client over a caller-supplied credentials
// provider, which is how the tests drive it with static credentials instead of a live AWS account.
// The provider is wrapped in aws.NewCredentialsCache so repeated requests do not re-retrieve
// credentials that have not expired; passing an already-cached provider (as cfg.Credentials from
// LoadDefaultConfig already is) is harmless, CredentialsCache tolerates nesting.
func NewSigV4HTTPClientWithCredentials(creds aws.CredentialsProvider, timeout time.Duration, region, signingName string) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &sigv4Transport{
			signer:      v4.NewSigner(),
			credentials: aws.NewCredentialsCache(creds),
			region:      region,
			signingName: signingName,
		},
	}
}
