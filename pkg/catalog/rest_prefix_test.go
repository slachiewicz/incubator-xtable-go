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

// Package catalog_test covers docs/improvement-plan.md T53: negotiating an Iceberg REST catalog's
// GET /v1/config prefix before addressing any table, tables, or namespace path. The fixtures here
// model Microsoft OneLake's Iceberg REST endpoint closely enough to prove the gap without a real
// Fabric workspace: a non-empty, slash-bearing prefix, a read-only endpoint list, and pagination.
package catalog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/model"
)

// fakeRESTCatalog is a minimal Iceberg REST catalog server built for these tests. It answers
// GET /v1/config plus a fixed set of registered routes, and 404s -- while recording the path -- on
// anything else. Recording the miss, rather than only asserting the hit, is what proves a wrong
// (unprefixed or doubly-prefixed) path was never built, not merely that a right one sometimes was.
type fakeRESTCatalog struct {
	mu sync.Mutex

	configPrefix         string   // goes under "overrides.prefix"
	configDefaultsPrefix string   // goes under "defaults.prefix"
	configEndpoints      []string // nil omits the "endpoints" field entirely
	configStatus         int      // 0 defaults to 200

	configHits      int32
	configWarehouse string

	routes map[string]http.HandlerFunc // "METHOD /path" -> handler

	unmatched []string
}

func newFakeRESTCatalog() *fakeRESTCatalog {
	return &fakeRESTCatalog{routes: make(map[string]http.HandlerFunc)}
}

func (f *fakeRESTCatalog) on(method, path string, h http.HandlerFunc) {
	f.routes[method+" "+path] = h
}

func (f *fakeRESTCatalog) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config" {
			atomic.AddInt32(&f.configHits, 1)
			f.mu.Lock()
			f.configWarehouse = r.URL.Query().Get("warehouse")
			status := f.configStatus
			f.mu.Unlock()

			if status != 0 && status != http.StatusOK {
				w.WriteHeader(status)
				return
			}

			body := map[string]any{}
			if f.configDefaultsPrefix != "" {
				body["defaults"] = map[string]any{"prefix": f.configDefaultsPrefix}
			}
			if f.configPrefix != "" || f.configEndpoints != nil {
				overrides := map[string]any{}
				if f.configPrefix != "" {
					overrides["prefix"] = f.configPrefix
				}
				body["overrides"] = overrides
			}
			if f.configEndpoints != nil {
				body["endpoints"] = f.configEndpoints
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(body)
			return
		}

		if h, ok := f.routes[r.Method+" "+r.URL.Path]; ok {
			h(w, r)
			return
		}

		f.mu.Lock()
		f.unmatched = append(f.unmatched, r.Method+" "+r.URL.Path)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}))
}

func (f *fakeRESTCatalog) configHitCount() int {
	return int(atomic.LoadInt32(&f.configHits))
}

func (f *fakeRESTCatalog) warehouseSeen() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.configWarehouse
}

func (f *fakeRESTCatalog) unmatchedRequests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.unmatched))
	copy(out, f.unmatched)
	return out
}

func loadTableJSON(location string) string {
	return fmt.Sprintf(`{"metadata":{"location":%q}}`, location)
}

// 1. Prefix negotiation happens and is used: a non-empty, slash-bearing prefix (OneLake's shape)
// is folded into every later path.
func TestIcebergRESTPrefixNegotiationIsUsedInTablePath(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.configPrefix = "ws-guid/item-guid"
	fake.on(http.MethodGet, "/v1/ws-guid/item-guid/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loadTableJSON("abfss://onelake/dbo/orders")))
	})
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
	got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

	require.NoError(t, err)
	assert.Equal(t, "abfss://onelake/dbo/orders", got.BasePath)
	assert.Empty(t, fake.unmatchedRequests(), "every request must land on the negotiated, prefixed path")
}

// 2. The configured warehouse reaches the config call as a query parameter, exactly as configured.
func TestIcebergRESTPrefixNegotiationSendsWarehouse(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.on(http.MethodGet, "/v1/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loadTableJSON("abfss://onelake/dbo/orders")))
	})
	server := fake.server()
	defer server.Close()

	cfg := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "dbo",
		URI:          server.URL,
		Properties:   map[string]string{catalog.PropCatalogWarehouse: "myworkspace/myitem"},
	}
	src, err := catalog.NewIcebergRESTConversionSource(cfg)
	require.NoError(t, err)

	_, err = src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})
	require.NoError(t, err)

	assert.Equal(t, "myworkspace/myitem", fake.warehouseSeen())
}

// 3. An empty prefix produces today's paths across every site T53 touched: no doubled slash, no
// stray segment. This is the regression pin for the tabulario/iceberg-rest and Nessie behavior the
// dockertest suite already exercises against a real container.
func TestIcebergRESTPrefixNegotiationEmptyPrefixUnchanged(t *testing.T) {
	t.Parallel()

	t.Run("load", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodGet, "/v1/namespaces/analytics/tables/events", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/events")))
		})
		server := fake.server()
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "analytics", "")
		_, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "analytics", Table: "events"})
		require.NoError(t, err)
		assert.Empty(t, fake.unmatchedRequests())
	})

	t.Run("create", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodPost, "/v1/namespaces/analytics/tables", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "analytics", "")
		table := sampleIcebergTable(t)
		require.NoError(t, client.CreateOrUpdateTable(context.Background(), table, nil))
		assert.Empty(t, fake.unmatchedRequests())
	})

	t.Run("commit update on conflict", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodPost, "/v1/namespaces/analytics/tables", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusConflict)
		})
		fake.on(http.MethodPost, "/v1/namespaces/analytics/tables/events", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "analytics", "")
		table := sampleIcebergTable(t)
		require.NoError(t, client.CreateOrUpdateTable(context.Background(), table, nil))
		assert.Empty(t, fake.unmatchedRequests())
	})

	t.Run("drop", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodDelete, "/v1/namespaces/analytics/tables/events", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "analytics", "")
		require.NoError(t, client.DropTable(context.Background(), "analytics", "events"))
		assert.Empty(t, fake.unmatchedRequests())
	})
}

// 4. Config is fetched once per client, not once per operation.
func TestIcebergRESTPrefixNegotiationFetchedOncePerClient(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.configPrefix = "ws/item"
	fake.on(http.MethodGet, "/v1/ws/item/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loadTableJSON("abfss://onelake/dbo/orders")))
	})
	fake.on(http.MethodGet, "/v1/ws/item/namespaces/dbo/tables/customers", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loadTableJSON("abfss://onelake/dbo/customers")))
	})
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")

	_, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})
	require.NoError(t, err)
	_, err = src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "customers"})
	require.NoError(t, err)

	assert.Equal(t, 1, fake.configHitCount(), "two operations must negotiate the prefix only once")
}

// 5. A 404 from /v1/config falls back to the empty prefix, and the operation still succeeds.
func TestIcebergRESTPrefixNegotiation404FallsBackToEmptyPrefix(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.configStatus = http.StatusNotFound
	fake.on(http.MethodGet, "/v1/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(loadTableJSON("s3://lake/dbo/orders")))
	})
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
	got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

	require.NoError(t, err)
	assert.Equal(t, "s3://lake/dbo/orders", got.BasePath)
	assert.Empty(t, fake.unmatchedRequests())
}

// 6a. Abandoning the sequence early -- a plain `for ... range { break }`, the idiomatic case Go's
// range-over-func contract has to support without panicking -- must stop the walk at both levels:
// within a namespace's own table pagination, and across the namespace loop itself. Two namespaces
// and two tables in the first prove both: ns2's handler must never be called, and "customers"
// (the first namespace's second table) must never be yielded.
func TestIcebergRESTConversionSourceListTablesStopsOnEarlyAbandonment(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.on(http.MethodGet, "/v1/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"namespaces": [["ns1"], ["ns2"]]}`))
	})
	fake.on(http.MethodGet, "/v1/namespaces/ns1/tables", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"identifiers": [{"namespace": ["ns1"], "name": "orders"}, {"namespace": ["ns1"], "name": "customers"}]}`))
	})
	fake.on(http.MethodGet, "/v1/namespaces/ns2/tables", func(w http.ResponseWriter, _ *http.Request) {
		t.Error("abandoning the sequence after the first identifier must stop the walk before it reaches a second namespace")
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "", "")

	var got []catalog.TableIdentifier
	require.NotPanics(t, func() {
		for id, err := range src.ListTables(context.Background(), "", catalog.TableFilter{}) {
			require.NoError(t, err)
			got = append(got, id)
			break
		}
	})

	assert.Equal(t, []catalog.TableIdentifier{{Database: "ns1", Table: "orders"}}, got,
		"only the first identifier from the first namespace must be yielded before the break stops the walk")
}

// 6. ListTables walks namespaces and tables, yields each identifier exactly once, and surfaces a
// mid-iteration error (from a failing second page) rather than truncating the listing silently.
func TestIcebergRESTConversionSourceListTablesWalksAndSurfacesMidIterationError(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.on(http.MethodGet, "/v1/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"namespaces": [["dbo"]]}`))
	})

	var tablesCalls int32
	fake.on(http.MethodGet, "/v1/namespaces/dbo/tables", func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&tablesCalls, 1) == 1 {
			require.Empty(t, r.URL.Query().Get("pageToken"))
			_, _ = w.Write([]byte(`{"identifiers": [{"namespace": ["dbo"], "name": "orders"}], "next-page-token": "page-2"}`))
			return
		}
		assert.Equal(t, "page-2", r.URL.Query().Get("pageToken"))
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "", "")

	var (
		yielded []catalog.TableIdentifier
		errs    []error
	)
	for id, err := range src.ListTables(context.Background(), "", catalog.TableFilter{}) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		yielded = append(yielded, id)
	}

	require.Len(t, yielded, 1, "the first page's table must be yielded before the failure")
	assert.Equal(t, catalog.TableIdentifier{Database: "dbo", Table: "orders"}, yielded[0])
	require.Len(t, errs, 1, "a failing page must surface exactly one error, not truncate silently")
	assert.Equal(t, int32(2), atomic.LoadInt32(&tablesCalls), "the paginator must have followed next-page-token once")
}

// ListTables also honors an explicit database, skipping the namespaces listing entirely.
func TestIcebergRESTConversionSourceListTablesHonorsExplicitDatabase(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.on(http.MethodGet, "/v1/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		t.Error("an explicit database must not trigger a namespaces listing")
		w.WriteHeader(http.StatusInternalServerError)
	})
	fake.on(http.MethodGet, "/v1/namespaces/dbo/tables", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"identifiers": [{"namespace": ["dbo"], "name": "orders"}, {"namespace": ["dbo"], "name": "customers"}]}`))
	})
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")

	var got []catalog.TableIdentifier
	for id, err := range src.ListTables(context.Background(), "dbo", catalog.TableFilter{}) {
		require.NoError(t, err)
		got = append(got, id)
	}

	assert.ElementsMatch(t, []catalog.TableIdentifier{
		{Database: "dbo", Table: "orders"},
		{Database: "dbo", Table: "customers"},
	}, got)
}

// 7. A 405 on a write, and a config response that positively advertises no write route, both
// produce an error naming the catalog as read-only and naming the refused operation -- not a bare
// status code.
func TestIcebergRESTReadOnlyCatalogRefusesWrites(t *testing.T) {
	t.Parallel()

	t.Run("pre-emptive refusal from a read-only endpoints list", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.configEndpoints = []string{
			"GET /v1/{prefix}/namespaces",
			"GET /v1/{prefix}/namespaces/{namespace}/tables",
			"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
			"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
		}
		var postSeen int32
		fake.on(http.MethodPost, "/v1/namespaces/dbo/tables", func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt32(&postSeen, 1)
			w.WriteHeader(http.StatusOK)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "dbo", "")
		err := client.CreateOrUpdateTable(context.Background(), sampleIcebergTable(t), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only")
		assert.Contains(t, err.Error(), "CreateOrUpdateTable")
		assert.Zero(t, atomic.LoadInt32(&postSeen), "a pre-emptive refusal must never issue the write request")
	})

	t.Run("runtime 405 on create", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodPost, "/v1/namespaces/dbo/tables", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "dbo", "")
		err := client.CreateOrUpdateTable(context.Background(), sampleIcebergTable(t), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only")
		assert.Contains(t, err.Error(), "CreateOrUpdateTable")
	})

	t.Run("runtime 405 on drop", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodDelete, "/v1/namespaces/dbo/tables/events", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "dbo", "")
		err := client.DropTable(context.Background(), "dbo", "events")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only")
		assert.Contains(t, err.Error(), "DropTable")
	})
}

// 8. Prefix negotiation is race-free: firing concurrent operations through one client results in
// exactly one config call, and -race reports nothing.
func TestIcebergRESTPrefixNegotiationConcurrentOperationsSingleFlightConfig(t *testing.T) {
	t.Parallel()

	fake := newFakeRESTCatalog()
	fake.configPrefix = "ws/item"

	const n = 32
	for i := 0; i < n; i++ {
		table := fmt.Sprintf("t%d", i)
		path := "/v1/ws/item/namespaces/dbo/tables/" + table
		location := "abfss://onelake/dbo/" + table
		fake.on(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(loadTableJSON(location)))
		})
	}
	server := fake.server()
	defer server.Close()

	src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: fmt.Sprintf("t%d", i)})
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	assert.Equal(t, 1, fake.configHitCount(), "concurrent first callers must single-flight into one config call")
}

// 9. defaults.prefix is honored, per the specification's defaults-then-overrides merge order.
// Nessie 0.108 and Lakekeeper 0.13 both send the prefix only under "defaults" and no
// "overrides.prefix" at all; before the fix polytable read only "overrides", computed an empty
// prefix, and 404'd every subsequent request against both.
func TestIcebergRESTConfigDefaultsPrefixHonored(t *testing.T) {
	t.Parallel()

	t.Run("only defaults.prefix is present and is used", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.configDefaultsPrefix = "nessie-prefix"
		fake.on(http.MethodGet, "/v1/nessie-prefix/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
		})
		server := fake.server()
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/orders", got.BasePath)
		assert.Empty(t, fake.unmatchedRequests(), "a defaults-only prefix must still be folded into the request path")
	})

	t.Run("only overrides.prefix is present, unchanged behavior", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.configPrefix = "override-prefix"
		fake.on(http.MethodGet, "/v1/override-prefix/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
		})
		server := fake.server()
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/orders", got.BasePath)
		assert.Empty(t, fake.unmatchedRequests())
	})

	t.Run("both present, overrides wins", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.configDefaultsPrefix = "defaults-prefix"
		fake.configPrefix = "overrides-prefix"
		fake.on(http.MethodGet, "/v1/overrides-prefix/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
		})
		server := fake.server()
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/orders", got.BasePath)
		assert.Empty(t, fake.unmatchedRequests(), "the specification's merge order puts overrides last, so it must win when both are present")
	})

	t.Run("neither present, empty prefix, today's behavior", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.on(http.MethodGet, "/v1/namespaces/dbo/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
		})
		server := fake.server()
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/orders", got.BasePath)
		assert.Empty(t, fake.unmatchedRequests())
	})
}

// uriRecorder captures the raw request line (RequestURI, as it arrived on the wire) of every
// request a test server sees. Prefix escaping bugs are only visible on the wire form: net/http
// decodes URL.Path for you, which would hide exactly the double-encoding this exists to catch.
type uriRecorder struct {
	mu   sync.Mutex
	uris []string
}

func (u *uriRecorder) record(r *http.Request) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.uris = append(u.uris, r.RequestURI)
}

func (u *uriRecorder) all() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]string, len(u.uris))
	copy(out, u.uris)
	return out
}

// lastNonConfigURI returns the last recorded request URI that is not a /v1/config call (with or
// without a query string), i.e. the table request the test actually cares about.
func lastNonConfigURI(uris []string) string {
	for i := len(uris) - 1; i >= 0; i-- {
		if !strings.HasPrefix(uris[i], "/v1/config") {
			return uris[i]
		}
	}
	return ""
}

// newEscapeProbeServer starts a fake catalog that answers GET /v1/config with overridesPrefix
// under "overrides.prefix" and answers every other request with a fixed load-table response,
// recording every request's raw wire URI along the way.
func newEscapeProbeServer(overridesPrefix string) (*httptest.Server, *uriRecorder) {
	rec := &uriRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		if r.URL.Path == "/v1/config" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"overrides": map[string]any{"prefix": overridesPrefix},
			})
			return
		}
		_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
	}))
	return server, rec
}

// 10. The negotiated prefix is passed through verbatim, not escaped, while namespace and table
// segments still are. Nessie serves its prefix already percent-encoded ("main%7Cwarehouse", the
// encoding of "main|warehouse"); escaping it again turns "%" into "%25" and 404s every request.
// OneLake serves "<workspace>/<item>" with a literal "/"; splitting the prefix on "/" and
// escaping each part happens to work for OneLake but breaks Nessie, so the fix treats the whole
// prefix as one opaque path component either way.
func TestIcebergRESTPrefixPassedThroughVerbatim(t *testing.T) {
	t.Parallel()

	t.Run("a pre-encoded prefix reaches the wire byte-identical", func(t *testing.T) {
		t.Parallel()

		server, rec := newEscapeProbeServer("main%7Cwarehouse")
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		_, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})
		require.NoError(t, err)

		tableURI := lastNonConfigURI(rec.all())
		require.NotEmpty(t, tableURI)
		assert.Equal(t, "/v1/main%7Cwarehouse/namespaces/dbo/tables/orders", tableURI,
			"re-escaping the prefix would turn its %7C into %2537C")
		assert.NotContains(t, tableURI, "%25")
	})

	t.Run("a prefix with a literal slash still produces path separators, not %2F", func(t *testing.T) {
		t.Parallel()

		server, rec := newEscapeProbeServer("workspace/item-guid")
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		_, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})
		require.NoError(t, err)

		tableURI := lastNonConfigURI(rec.all())
		require.NotEmpty(t, tableURI)
		assert.Equal(t, "/v1/workspace/item-guid/namespaces/dbo/tables/orders", tableURI)
		assert.NotContains(t, tableURI, "%2F")
	})

	t.Run("namespace and table segments are still escaped", func(t *testing.T) {
		t.Parallel()

		server, rec := newEscapeProbeServer("")
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "sales team", "")
		_, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "sales team", Table: "profit share"})
		require.NoError(t, err)

		tableURI := lastNonConfigURI(rec.all())
		require.NotEmpty(t, tableURI)
		assert.Equal(t, "/v1/namespaces/sales%20team/tables/profit%20share", tableURI,
			"the fix narrows escaping to namespace/table segments, it must not remove it there")
	})
}

// 11. A 404 from GET /v1/config is disambiguated rather than latched as "this catalog predates
// the endpoint". Polaris answers a typo'd warehouse with 404 even though /v1/config exists; the
// old code read any 404 as "no config endpoint" and silently used an empty prefix forever.
func TestIcebergRESTConfig404Disambiguation(t *testing.T) {
	t.Parallel()

	t.Run("typo'd warehouse: 404 with it, 200 without -> named error", func(t *testing.T) {
		t.Parallel()

		var mu sync.Mutex
		var hits int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			hits++
			mu.Unlock()
			if r.URL.Query().Get("warehouse") != "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{})
		}))
		defer server.Close()

		cfg := &catalog.Config{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: "dbo",
			URI:          server.URL,
			Properties:   map[string]string{catalog.PropCatalogWarehouse: "typo-warehouse"},
		}
		src, err := catalog.NewIcebergRESTConversionSource(cfg)
		require.NoError(t, err)

		_, err = src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "typo-warehouse", "the error must name the warehouse that could not be found")

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 2, hits, "the disambiguating request without the warehouse must have been made")
	})

	t.Run("genuinely old catalog: 404 with or without the warehouse -> falls back, no error", func(t *testing.T) {
		t.Parallel()

		var mu sync.Mutex
		var configHits int
		var tableHits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/config" {
				mu.Lock()
				configHits++
				mu.Unlock()
				w.WriteHeader(http.StatusNotFound)
				return
			}
			atomic.AddInt32(&tableHits, 1)
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
		}))
		defer server.Close()

		cfg := &catalog.Config{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: "dbo",
			URI:          server.URL,
			Properties:   map[string]string{catalog.PropCatalogWarehouse: "some-warehouse"},
		}
		src, err := catalog.NewIcebergRESTConversionSource(cfg)
		require.NoError(t, err)

		got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.NoError(t, err, "a catalog that 404s /v1/config unconditionally must fall back, not error")
		assert.Equal(t, "s3://lake/orders", got.BasePath)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 2, configHits, "both the warehouse-qualified and the disambiguating request must have run")
		assert.Equal(t, int32(1), atomic.LoadInt32(&tableHits))
	})

	t.Run("no warehouse configured: 404 -> falls back, exactly one config request", func(t *testing.T) {
		t.Parallel()

		var configHits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/config" {
				atomic.AddInt32(&configHits, 1)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(loadTableJSON("s3://lake/orders")))
		}))
		defer server.Close()

		src := catalog.NewIcebergRESTConversionSourceWithClient(server.Client(), server.URL, "dbo", "")
		got, err := src.GetSourceTable(context.Background(), catalog.TableIdentifier{Database: "dbo", Table: "orders"})

		require.NoError(t, err)
		assert.Equal(t, "s3://lake/orders", got.BasePath)
		assert.Equal(t, int32(1), atomic.LoadInt32(&configHits),
			"with no warehouse configured there is nothing to disambiguate, so no second request should fire")
	})
}

// 12. The read-only heuristic ignores a "/metrics" write route. Unity Catalog OSS is read-only
// (GET/HEAD only) but advertises "POST .../tables/{table}/metrics"; before the fix that route
// alone made writeEndpointAdvertised report the catalog as writable.
func TestIcebergRESTReadOnlyHeuristicIgnoresMetricsEndpoint(t *testing.T) {
	t.Parallel()

	t.Run("a catalog advertising only the metrics POST stays read-only", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.configEndpoints = []string{
			"GET /v1/{prefix}/namespaces",
			"HEAD /v1/{prefix}/namespaces/{namespace}",
			"GET /v1/{prefix}/namespaces/{namespace}/tables",
			"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
			"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
			"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
		}
		var postSeen int32
		fake.on(http.MethodPost, "/v1/namespaces/dbo/tables", func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt32(&postSeen, 1)
			w.WriteHeader(http.StatusOK)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "dbo", "")
		err := client.CreateOrUpdateTable(context.Background(), sampleIcebergTable(t), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only")
		assert.Contains(t, err.Error(), "CreateOrUpdateTable")
		assert.Zero(t, atomic.LoadInt32(&postSeen), "a metrics-only endpoints list must never be mistaken for table write support")
	})

	t.Run("a genuine table write route alongside the metrics POST is still detected", func(t *testing.T) {
		t.Parallel()

		fake := newFakeRESTCatalog()
		fake.configEndpoints = []string{
			"GET /v1/{prefix}/namespaces",
			"GET /v1/{prefix}/namespaces/{namespace}/tables",
			"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
			"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
			"POST /v1/{prefix}/namespaces/{namespace}/tables",
		}
		var postSeen int32
		fake.on(http.MethodPost, "/v1/namespaces/dbo/tables", func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt32(&postSeen, 1)
			w.WriteHeader(http.StatusOK)
		})
		server := fake.server()
		defer server.Close()

		client := catalog.NewIcebergRESTCatalogClientWithHTTPClient(server.Client(), server.URL, "dbo", "")
		err := client.CreateOrUpdateTable(context.Background(), sampleIcebergTable(t), nil)

		require.NoError(t, err, "excluding the metrics route must not disable write detection generally")
		assert.Equal(t, int32(1), atomic.LoadInt32(&postSeen))
	})
}

// sampleIcebergTable builds a minimal, valid *model.Table for CreateOrUpdateTable subtests.
func sampleIcebergTable(t *testing.T) *model.Table {
	t.Helper()
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema("events", []*model.Field{idField}, false)
	return &model.Table{
		Name:             "events",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         "s3://my-bucket/events",
		LatestCommitTime: time.Now().UnixMilli(),
	}
}
