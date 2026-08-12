#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SPEC_FILE = rest-service-open-api.yaml

.PHONY: help lint generate-go clean

help:
	@echo "Available targets:"
	@echo "  lint        - Validate the OpenAPI specification"
	@echo "  generate-go - Generate Go models and server routes using oapi-codegen / openapi-generator"
	@echo "  clean       - Clean generated files"

lint:
	@if command -v openapi-spec-validator >/dev/null 2>&1; then \
		openapi-spec-validator $(SPEC_FILE); \
	elif command -v vacuum >/dev/null 2>&1; then \
		vacuum lint $(SPEC_FILE); \
	else \
		echo "Please install openapi-spec-validator or vacuum to lint $(SPEC_FILE)"; \
	fi

generate-go:
	@if command -v oapi-codegen >/dev/null 2>&1; then \
		oapi-codegen -package spec -generate types,server $(SPEC_FILE) > types.gen.go; \
	elif command -v openapi-generator >/dev/null 2>&1; then \
		openapi-generator generate -i $(SPEC_FILE) -g go-server -o gen/go; \
	else \
		echo "Please install oapi-codegen (go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest)"; \
	fi

clean:
	rm -f types.gen.go
	rm -rf gen/
