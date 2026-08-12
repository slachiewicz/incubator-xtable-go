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
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/slachiewicz/xtable-go/pkg/conversion"
	"github.com/slachiewicz/xtable-go/pkg/daemon"
)

var (
	version = "0.1.0-SNAPSHOT"
)

func main() {
	var (
		configPath   string
		port         int
		interval     time.Duration
		enableDaemon bool
	)

	rootCmd := &cobra.Command{
		Use:     "xtable-service",
		Version: version,
		Short:   "Apache XTable REST Service & Continuous Synchronization Daemon",
		Long:    `Runs the Apache XTable REST API server and continuous background synchronization daemon.`,
		// A failure to bind or read a config is not a usage error; printing the flag list on top of
		// the real message only buries it.
		SilenceUsage: true,
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

			// 2. Bind before announcing. Printing "Listening" and only then discovering the port is
			// taken would advertise an address that never worked.
			listener, err := net.Listen("tcp", httpServer.Addr)
			if err != nil {
				return fmt.Errorf("cannot listen on port %d: %w", port, err)
			}

			// The banner goes to stdout deliberately. slog writes to stderr, so a plain
			// `xtable-service > log.txt` used to look like the process had produced nothing at all.
			printStartupBanner(cmd.OutOrStdout(), port, configPath, enableDaemon, interval)

			serverErr := make(chan error, 1)
			go func() {
				if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					serverErr <- err
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

			// Wait for an interrupt, or for the listener to fail. Without the second case a port
			// clash left the process alive and silent, looking healthy but serving nothing.
			select {
			case <-ctx.Done():
			case err := <-serverErr:
				return fmt.Errorf("HTTP server failed: %w", err)
			}

			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nShutting down XTable service...")

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := httpServer.Shutdown(shutdownCtx); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Stopped.")
			return nil
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

// printStartupBanner tells the operator what is actually running: where to reach it, which endpoints
// exist, whether background sync is on, and how to stop it.
func printStartupBanner(w io.Writer, port int, configPath string, daemonEnabled bool, interval time.Duration) {
	_, _ = fmt.Fprintf(w, "xtable-go service %s\n", version)
	_, _ = fmt.Fprintf(w, "  Listening      http://localhost:%d\n", port)
	_, _ = fmt.Fprintf(w, "  Endpoints      GET  /v1/health\n")
	_, _ = fmt.Fprintf(w, "                 POST /v1/conversion/table\n")
	_, _ = fmt.Fprintf(w, "                 GET  /v1/conversion/table/{id}\n")
	_, _ = fmt.Fprintf(w, "                 POST /v1/conversion/inspect\n")

	switch {
	case configPath == "" && daemonEnabled:
		_, _ = fmt.Fprintf(w, "  Background     disabled - --daemon was given without --config, so there is nothing to sync\n")
	case configPath == "":
		_, _ = fmt.Fprintf(w, "  Background     disabled - REST API only (pass --config to enable continuous sync)\n")
	default:
		_, _ = fmt.Fprintf(w, "  Background     every %s from %s\n", interval, configPath)
	}

	_, _ = fmt.Fprintf(w, "  Stop           Ctrl+C\n\n")
}
