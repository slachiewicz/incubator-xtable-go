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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/apache/incubator-xtable-go/pkg/conversion"
	"github.com/apache/incubator-xtable-go/pkg/daemon"
)

var (
	version = "0.1.0-SNAPSHOT"
)

func main() {
	var (
		configPath string
		port       int
		interval   time.Duration
		enableDaemon bool
	)

	rootCmd := &cobra.Command{
		Use:   "xtable-service",
		Short: "Apache XTable REST Service & Continuous Synchronization Daemon",
		Long:  `Runs the Apache XTable REST API server and continuous background synchronization daemon.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			logger := slog.Default()

			// 1. Initialize REST Server
			restServer := daemon.NewServer(version)
			httpServer := &http.Server{
				Addr:         fmt.Sprintf(":%d", port),
				Handler:      restServer.Handler(),
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 60 * time.Second,
			}

			// 2. Start HTTP Server in background
			go func() {
				logger.Info(fmt.Sprintf("🚀 Starting XTable REST Service on port :%d", port))
				if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("HTTP server error", "error", err)
				}
			}()

			// 3. Start Background Daemon if config is supplied
			if configPath != "" || enableDaemon {
				if configPath == "" {
					logger.Warn("No config file provided for daemon; running REST API only")
				} else {
					data, err := os.ReadFile(configPath)
					if err != nil {
						return fmt.Errorf("failed to read config file %s: %w", configPath, err)
					}

					var cfg conversion.Config
					if strings.HasSuffix(configPath, ".json") {
						if err := json.Unmarshal(data, &cfg); err != nil {
							return fmt.Errorf("failed to parse JSON config: %w", err)
						}
					} else {
						if err := yaml.Unmarshal(data, &cfg); err != nil {
							return fmt.Errorf("failed to parse YAML config: %w", err)
						}
					}

					d := daemon.NewDaemon(&cfg, interval, logger)
					go func() {
						if err := d.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
							logger.Error("Daemon sync error", "error", err)
						}
					}()
				}
			}

			// Wait for interrupt
			<-ctx.Done()
			logger.Info("Shutting down XTable service...")

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return httpServer.Shutdown(shutdownCtx)
		},
	}

	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to YAML/JSON dataset configuration file")
	rootCmd.Flags().IntVarP(&port, "port", "p", 8080, "HTTP port for REST API")
	rootCmd.Flags().DurationVarP(&interval, "interval", "i", 30*time.Second, "Polling interval for daemon synchronization")
	rootCmd.Flags().BoolVarP(&enableDaemon, "daemon", "d", false, "Enable background continuous sync daemon")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
