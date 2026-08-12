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

package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/incubator-xtable-go/pkg/conversion"
	"github.com/apache/incubator-xtable-go/pkg/io"
	"github.com/apache/incubator-xtable-go/pkg/spi"
)

// Daemon coordinates continuous, scheduled synchronization across configured lakehouse datasets.
type Daemon struct {
	config   *conversion.Config
	interval time.Duration
	logger   *slog.Logger
}

// NewDaemon creates a new continuous synchronization daemon.
func NewDaemon(cfg *conversion.Config, interval time.Duration, logger *slog.Logger) *Daemon {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Daemon{
		config:   cfg,
		interval: interval,
		logger:   logger,
	}
}

// Start begins the continuous synchronization loop until the context is cancelled.
func (d *Daemon) Start(ctx context.Context) error {
	d.logger.Info("Starting continuous XTable synchronization daemon", "interval", d.interval, "datasets", len(d.config.Datasets))

	// Initial sync pass
	d.syncAll(ctx)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Stopping continuous XTable synchronization daemon")
			return ctx.Err()
		case <-ticker.C:
			d.syncAll(ctx)
		}
	}
}

func (d *Daemon) syncAll(ctx context.Context) {
	for i, ds := range d.config.Datasets {
		if ctx.Err() != nil {
			return
		}

		// Propagate top-level defaults
		if ds.SourceFormat == "" && d.config.SourceFormat != "" {
			ds.SourceFormat = d.config.SourceFormat
		}
		if len(ds.TargetFormats) == 0 && len(d.config.TargetFormats) > 0 {
			ds.TargetFormats = d.config.TargetFormats
		}

		d.logger.Debug(fmt.Sprintf("[%d/%d] Checking table '%s' for updates", i+1, len(d.config.Datasets), ds.TableName))

		storage, err := io.NewStorageForPath(ctx, ds.TableBasePath)
		if err != nil {
			d.logger.Error("Failed to initialize storage", "path", ds.TableBasePath, "error", err)
			continue
		}

		controller := conversion.NewController(storage)
		results, err := controller.Sync(ctx, ds)
		if err != nil {
			d.logger.Error("Sync failed", "table", ds.TableName, "error", err)
			continue
		}

		for targetFormat, res := range results {
			if res.StatusCode == spi.SyncStatusSuccess {
				d.logger.Info("Table synchronized successfully",
					"table", ds.TableName,
					"source", ds.SourceFormat,
					"target", targetFormat,
					"duration", res.Duration,
					"lastInstant", res.LastInstantSynced)
			} else {
				d.logger.Error("Table sync error",
					"table", ds.TableName,
					"target", targetFormat,
					"error", res.Error)
			}
		}
	}
}
