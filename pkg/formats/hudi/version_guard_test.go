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

package hudi_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/hudi"
	"github.com/slachiewicz/polytable/pkg/io"
)

func TestReadProperties_TableVersionGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{name: "version 6 is the supported floor", version: "6", wantErr: false},
		{name: "version 8 is Hudi 1.0", version: "8", wantErr: true},
		{name: "version 9 is Hudi 1.1+", version: "9", wantErr: true},
		{name: "missing version does not block", version: "", wantErr: false},
		{name: "unparseable version does not block", version: "eight", wantErr: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			storage := io.NewMemoryStorage()
			base := "mem://guard"

			props := "hoodie.table.name=t\nhoodie.table.type=COPY_ON_WRITE\n"
			if tt.version != "" {
				props += fmt.Sprintf("hoodie.table.version=%s\n", tt.version)
			}
			require.NoError(t, storage.Write(ctx, base+"/.hoodie/hoodie.properties", []byte(props)))

			_, err := hudi.NewSource(storage, base).ReadProperties(ctx)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "Hudi 1.x")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
