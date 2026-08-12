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

package model

// FilesDiff captures the addition and removal of data files between two table states or commits.
type FilesDiff struct {
	// FilesAdded is the list of new or updated data files added in this commit.
	FilesAdded []*DataFile `json:"filesAdded,omitempty"`
	// FilesRemoved is the list of data files removed or compacted away in this commit.
	FilesRemoved []*DataFile `json:"filesRemoved,omitempty"`
}

// NewFilesDiff creates a new FilesDiff struct.
func NewFilesDiff(added, removed []*DataFile) *FilesDiff {
	return &FilesDiff{
		FilesAdded:   added,
		FilesRemoved: removed,
	}
}

// HasChanges returns true if there are any added or removed files.
func (d *FilesDiff) HasChanges() bool {
	if d == nil {
		return false
	}
	return len(d.FilesAdded) > 0 || len(d.FilesRemoved) > 0
}

// DiffFiles computes the difference between an old set and a new set of data files.
func DiffFiles(oldFiles, newFiles []*DataFile) *FilesDiff {
	oldMap := make(map[string]*DataFile, len(oldFiles))
	for _, f := range oldFiles {
		oldMap[f.PhysicalPath] = f
	}

	newMap := make(map[string]*DataFile, len(newFiles))
	for _, f := range newFiles {
		newMap[f.PhysicalPath] = f
	}

	var added []*DataFile
	for path, file := range newMap {
		if _, exists := oldMap[path]; !exists {
			added = append(added, file)
		}
	}

	var removed []*DataFile
	for path, file := range oldMap {
		if _, exists := newMap[path]; !exists {
			removed = append(removed, file)
		}
	}

	return &FilesDiff{
		FilesAdded:   added,
		FilesRemoved: removed,
	}
}
