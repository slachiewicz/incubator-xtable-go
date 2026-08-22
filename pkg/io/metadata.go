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
	"path/filepath"
	"strings"
)

// metadataDirNames are directory names, matched exactly against a single path component, that
// mark a format's metadata rather than data — for the formats whose metadata directory does not
// already start with "_" or "." (isConventionallyHidden below covers those).
//
// A polytable-synced directory holds every target's metadata side by side with the data files, by
// design (see docs/improvement-plan.md T45), so a source that crawls a directory for data files
// has to know the *whole* format registry's layout, not just its own. Each entry below is read
// from the writer, not guessed:
var metadataDirNames = map[string]struct{}{
	// Iceberg: table metadata JSON, manifests, manifest lists and version-hint.text all live
	// directly under "metadata" (pkg/formats/iceberg/source.go, target.go).
	"metadata": {},
	// Paimon: schema/schema-<id> (pkg/formats/paimon/manifest.go: schemaDir).
	"schema": {},
	// Paimon: snapshot/snapshot-<id>, plus the LATEST/EARLIEST hint files
	// (pkg/formats/paimon/manifest.go: snapshotDir).
	"snapshot": {},
	// Paimon: manifest/manifest-<uuid>-<n> and manifest-list-<uuid>-<n>
	// (pkg/formats/paimon/manifest.go: manifestDir).
	"manifest": {},
}

// IsMetadataPathComponent reports whether name — a single path component, never a suffix or a
// full path — marks a directory (or file) that a directory-crawling ConversionSource must treat
// as another format's metadata rather than as a data file or a Hive partition segment.
//
// Two rules, combined:
//
//  1. A leading "_" or "." excludes the component outright. This is the Hive/Spark convention for
//     "hidden" table entries (Hadoop's FileInputFormat.hiddenFileFilter, which Spark's own table
//     scan inherits), so adopting it here excludes nothing that a Hive- or Spark-compatible reader
//     would have surfaced anyway. One rule covers Delta's "_delta_log", the Parquet target's own
//     "_polytable_metadata", Hadoop's "_temporary" staging directory and "_SUCCESS" marker, and
//     Hudi's ".hoodie" — plus any future format that follows the same convention, without needing
//     a new name added here for it.
//
//     The tradeoff is deliberate and recorded rather than hidden: a directory using a raw,
//     non-"key=value" segment that happens to start with "_" or "." would also be excluded even
//     if a table genuinely partitioned data under it. That is accepted because Hive-style
//     partitioning always uses "key=value" segments — a bare "_foo" segment was never a valid
//     Hive partition value — and because the alternative (an exact-name list only) silently
//     admits every future format's underscore- or dot-prefixed metadata as data until someone
//     remembers to extend the list.
//
//  2. An exact-name list, metadataDirNames, for the metadata directories that do not follow that
//     convention: Iceberg's "metadata" and Paimon's "schema", "snapshot" and "manifest". These are
//     matched case-sensitively against one component only, so a Hive partition directory such as
//     "region=metadata" (the "=" is what makes it a valid partition segment) is untouched — only a
//     bare "metadata" component is excluded.
func IsMetadataPathComponent(name string) bool {
	if name == "" {
		return false
	}
	if strings.HasPrefix(name, "_") || strings.HasPrefix(name, ".") {
		return true
	}
	_, known := metadataDirNames[name]
	return known
}

// IsMetadataPath reports whether path — relative to a dataset's base path — has any component
// IsMetadataPathComponent excludes. A directory-crawling source has to check every component, not
// just the final one: "data/_delta_log/x.parquet" is metadata even though "x.parquet" itself is an
// unremarkable data file name, because the containing directory is Delta's log, not the caller's.
//
// path is expected relative to the base path a listing was rooted at (for example the output of
// RelativizePath), so that a base path which itself happens to contain "_" or "." ahead of the
// table root — an unrelated concern of the caller's filesystem layout — is never consulted.
func IsMetadataPath(path string) bool {
	// filepath.ToSlash is a no-op wherever the separator is already "/", and normalizes it where
	// a local-filesystem path was built with filepath.Join instead.
	for _, component := range strings.Split(filepath.ToSlash(path), "/") {
		if IsMetadataPathComponent(component) {
			return true
		}
	}
	return false
}
