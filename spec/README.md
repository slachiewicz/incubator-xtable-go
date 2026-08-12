<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# xtable-go Service OpenAPI Specification

The [`rest-service-open-api.yaml`](./spec/rest-service-open-api.yaml) defines the API contract for running table format conversions, polling conversion jobs, and inspecting lakehouse table metadata over HTTP/REST.

---

## 1. Specification Overview

- **Specification Standard**: OpenAPI 3.0.3
- **Supported Formats**: `DELTA`, `ICEBERG`, `HUDI`, `PAIMON`, `PARQUET`
- **Key Endpoints**:
  - `POST /v1/conversion/table`: Initiates zero-copy metadata translation (synchronous or asynchronous via `Prefer: respond-async`).
  - `GET /v1/conversion/table/{conversionId}`: Polls execution status and results for async conversions.
  - `POST /v1/conversion/inspect`: Discovers schema, partitions, and active data files for any supported table format.
  - `GET /v1/health`: Service health check.

---

## 2. Validation & Code Generation

### Validate OpenAPI Spec
```bash
make lint
```

### Generate Go REST Server & Client Models
```bash
make generate-go
```
