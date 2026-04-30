# DFTP-DACP Design Document

[中文](./DFTP-DACP-Design-Document-ZH.md) | [English](./DFTP-DACP-Design-Document.md)

## Table of Contents

- [1. Document Overview](#1-document-overview)
  - [1.1 Background and Motivation](#11-background-and-motivation)
  - [1.2 Design Goals](#12-design-goals)
- [2. Overall Architecture](#2-overall-architecture)
  - [2.1 Layered Architecture](#21-layered-architecture)
  - [2.2 Core Components](#22-core-components)
    - [2.2.1 DftpServer (Service Container)](#221-dftpserver-service-container)
    - [2.2.2 KernelModule (Protocol Routing Core)](#222-kernelmodule-protocol-routing-core)
    - [2.2.3 Client Gateway (Client Entry Layer)](#223-client-gateway-client-entry-layer)
    - [2.2.4 TicketManager (Ticket Lifecycle Management)](#224-ticketmanager-ticket-lifecycle-management)
    - [2.2.5 Action Handler Family (Control-Plane Handlers)](#225-action-handler-family-control-plane-handlers)
    - [2.2.6 Stream Handler Family (Data-Plane Handlers)](#226-stream-handler-family-data-plane-handlers)
    - [2.2.7 DacpCatalogModule (Catalog and Metadata)](#227-dacpcatalogmodule-catalog-and-metadata)
    - [2.2.8 DacpCookModule (Job Execution)](#228-dacpcookmodule-job-execution)
    - [2.2.9 DacpServerProxy (Proxy Forwarding)](#229-dacpserverproxy-proxy-forwarding)
    - [2.2.10 Packaging/ServerStart (Assembly and Deployment Entry)](#2210-packagingserverstart-assembly-and-deployment-entry)
- [3. Protocol Design](#3-protocol-design)
  - [3.1 Core Concepts](#31-core-concepts)
  - [3.2 Unified Interaction Model (Control Plane + Data Plane)](#32-unified-interaction-model-control-plane--data-plane)
  - [3.3 Authentication](#33-authentication)
  - [3.4 GET: Forward Provisioning (Fetch Data)](#34-get-forward-provisioning-fetch-data)
  - [3.5 PUT: Reverse of Forward Provisioning (Upload Data/Version)](#35-put-reverse-of-forward-provisioning-upload-dataversion)
  - [3.6 COOK: Reverse Provisioning (Send Algorithms to Data)](#36-cook-reverse-provisioning-send-algorithms-to-data)
    - [3.6.1 Recipe (Conceptual Model)](#361-recipe-conceptual-model)
    - [3.6.2 COOK Output](#362-cook-output)
    - [3.6.3 Integration Points with Operator Repository](#363-integration-points-with-operator-repository)
    - [3.6.4 Mapping to Current Implementation](#364-mapping-to-current-implementation)
- [4. Extension Design](#4-extension-design)
  - [4.1 Catalog Extension (Directory/Metadata)](#41-catalog-extension-directorymetadata)
  - [4.2 Cook Extension (Executors/Operators)](#42-cook-extension-executorsoperators)
  - [4.3 Proxy Extension (Protocol-Level Proxy)](#43-proxy-extension-protocol-level-proxy)
- [5. Data Model Design](#5-data-model-design)
  - [5.1 DataFrame / Row / StructType](#51-dataframe--row--structtype)
  - [5.2 Blob](#52-blob)
  - [5.3 TransformOp / Flow / Job](#53-transformop--flow--job)
- [6. Security Design](#6-security-design)
  - [6.1 Authentication and Identity](#61-authentication-and-identity)
  - [6.2 Authorization and Least Privilege (for COOK)](#62-authorization-and-least-privilege-for-cook)
  - [6.3 Isolation and Sandbox (Planned)](#63-isolation-and-sandbox-planned)
  - [6.4 Auditing and Observability](#64-auditing-and-observability)
- [7. Extensibility Design](#7-extensibility-design)
  - [7.1 Modular Extension Mechanism (System Core)](#71-modular-extension-mechanism-system-core)
  - [7.2 Extension Points (Implementation Guidance)](#72-extension-points-implementation-guidance)
  - [7.3 Extension Lifecycle and Assembly Flow](#73-extension-lifecycle-and-assembly-flow)
  - [7.4 Extension Priority and Conflict Handling](#74-extension-priority-and-conflict-handling)
  - [7.5 Stability Boundaries and Compatibility Constraints](#75-stability-boundaries-and-compatibility-constraints)
  - [7.6 Extension Implementation Template (Engineering Convention)](#76-extension-implementation-template-engineering-convention)
- [8. Engineering Implementation View](#8-engineering-implementation-view)
  - [8.1 Module Dependency Relationship (Maven)](#81-module-dependency-relationship-maven)
  - [8.2 Key Sequences (Implementation Constraints)](#82-key-sequences-implementation-constraints)
  - [8.3 Configuration Matrix (Phase 1)](#83-configuration-matrix-phase-1)
  - [8.4 Exception and Error Semantics (Recommended)](#84-exception-and-error-semantics-recommended)
  - [8.5 Test Strategy (Aligned by Module)](#85-test-strategy-aligned-by-module)
- [9. Summary](#9-summary)

## 1. Document Overview

### 1.1 Background and Motivation

In scientific computing and distributed data collaboration scenarios, the core pain points in data usage are:

- **Data scale keeps growing**: from GB to TB/PB, making data movement expensive in network, storage, and time.
- **Data usability keeps getting harder**: mixed structured and semi-structured data, plus script-heavy adaptation in traditional FTP/HTTP workflows.

The project therefore needs a protocol stack that provides:

- **Web-like access simplicity**: unified URL semantics and request/response model.
- **Local-like performance**: column pruning, parallel reading, and compression-friendly transfer.
- **Streaming-first processing**: consume and compute as soon as the first frame arrives.

This project uses a protocol-stack architecture:

- **DFTP (Data Frame Transfer Protocol)**: core transport and control-plane capabilities (authentication, GET/PUT/Action, ticket-based streaming), built on Arrow Flight.
- **DACP (Data Access & Collaboration Protocol)**: upper-layer capabilities on top of DFTP runtime/modules, including Catalog, Cook/COOK, permissions, and proxy.

### 1.2 Design Goals

- **Unified data abstraction**: `DataFrame` for structured streams and `Blob` for unstructured binary streams.
- **2D streaming frame (SDF) model**: schema-aware multi-row/multi-column transmission unit for frame/row-level consumption.
- **High-performance transfer**: based on Apache Arrow Flight for low-overhead, high-throughput exchange.
- **Column pruning**: fetch only needed columns to reduce IO/network cost.
- **Resume/replay**: frame-level recovery and re-fetch with unified resource identifiers.
- **Control/data-plane decoupling**: control plane returns `ticket + metadata`; data plane transfers by ticket.
- **Reverse provisioning (COOK)**: run algorithms near data and return compact results.
- **Security and isolation**: authentication, least privilege, auditable access, optional sandboxing.
- **Extensibility**: modular extension points for authentication, GET/PUT, Action, catalog, and executors.

## 2. Overall Architecture

### 2.1 Layered Architecture

1. **Data and Operator Abstraction Layer (`dftp-common`)**
   - Data structures: `DataFrame`, `Row`, `StructType`, `Blob`, `DataFrameMetaData`
   - Execution expression: `TransformOp` (Source/Map/Filter/Limit/Select)
   - Common capabilities: codecs, conversion, authentication models

2. **DFTP Core Protocol Layer (`dftp-server` / `dftp-client`)**
   - Server: `DftpServer` as unified container over Arrow Flight Server
   - Client: `DftpClient` for auth, Action calls, GET/PUT streaming
   - Mechanism: ticket registration and streaming (control/data decoupling)

3. **DACP Extension Layer (`catalog-module` / `cook-module` / `dacp-proxy`)**
   - Catalog: discovery, metadata, schema/docs/stats
   - Cook: recipe/flow submission, status/progress/result query
   - Proxy: protocol forwarding and ticket re-registration

4. **Client and Distribution Layer (`dacp-client` / `packaging`)**
   - `DacpClient`: high-level API for Catalog/Cook
   - `packaging`: distributable bundles and config templates

### 2.2 Core Components

#### 2.2.1 DftpServer (Service Container)

Responsibilities:

- Start/stop Arrow Flight Server
- Provide `ServerContext` (host/port/scheme, ticket registration, dftpHome, key materials)
- Load and initialize modules

Key interface: `DftpServer.start(...)`  
Primary dependencies: `dftp-server`, `KernelModule`, `DftpModule`

#### 2.2.2 KernelModule (Protocol Routing Core)

Responsibilities:

- Aggregate and route four extension handler families
- Unify dispatch between control plane and data plane
- Keep kernel stable while allowing pluggable business logic

Key interfaces: `AuthenticationMethod`, `ActionMethod`, `GetStreamMethod`, `PutStreamMethod`  
Primary dependencies: `dftp-server`, `dftp-common`

#### 2.2.3 Client Gateway (Client Entry Layer)

Responsibilities:

- Provide unified client entry for auth, Action, and streaming
- Encapsulate Flight connections and remote data proxies
- Shield upper layers from low-level protocol details

Key interfaces: `DftpClient`, `DacpClient`, `RemoteDataFrameProxy`  
Primary dependencies: `dftp-client`, `dacp-client`, `dftp-common`

#### 2.2.4 TicketManager (Ticket Lifecycle Management)

Responsibilities:

- Manage `ticket -> DataFrameHandle/BlobHandle` mappings
- Handle expiration, cleanup, and invalidation
- Support proxy-side ticket re-registration

Key interface: ticket registration/query via `ServerContext`  
Primary dependencies: `DftpServer`, `KernelModule`, `dftp-common`

#### 2.2.5 Action Handler Family (Control-Plane Handlers)

Responsibilities:

- Handle GET/PUT negotiation and Catalog/Cook/Proxy control actions
- Return JSON or `ticket + metadata`
- Enforce validation, authorization, and error normalization

Key interface: `ActionMethod` and module-specific action enums  
Primary dependencies: `KernelModule`, `catalog-module`, `cook-module`, `dacp-proxy`

#### 2.2.6 Stream Handler Family (Data-Plane Handlers)

Responsibilities:

- Execute actual stream reads/writes
- Transfer Arrow RecordBatch streams via ticket
- Hide transmission details for DataFrame/Blob

Key interfaces: `GetStreamMethod`, `PutStreamMethod`  
Primary dependencies: `KernelModule`, `dftp-common`, data source modules

#### 2.2.7 DacpCatalogModule (Catalog and Metadata)

Responsibilities:

- Provide dataset/dataframe discovery, schema, docs, and stats
- Output metadata in JSON-LD/RDF
- Decouple catalog backends via SPI

Key interfaces: `DacpCatalogModule`, `CatalogService`, `CatalogFormatter`  
Primary dependencies: `catalog-module`, `dftp-server`, `dftp-common`

#### 2.2.8 DacpCookModule (Job Execution)

Responsibilities:

- Handle `SUBMIT_RECIPE`, `SUBMIT_FLOW`, and job status actions
- Schedule TransformTree/Flow execution
- Publish result tickets for client stream retrieval

Key interfaces: `DacpCookModule`, `FlowScheduler`, `FlowExecutionContext`  
Primary dependencies: `cook-module`, `dftp-server`, `dftp-common`

#### 2.2.9 DacpServerProxy (Proxy Forwarding)

Responsibilities:

- Provide unified frontend gateway to backend DACP services
- Re-register tickets for result-oriented requests
- Hide backend topology and internal addresses

Key interface: `DacpServerProxy`  
Primary dependencies: `dacp-proxy`, `dftp-server`, `dftp-client`

#### 2.2.10 Packaging/ServerStart (Assembly and Deployment Entry)

Responsibilities:

- Provide startup scripts (`dftp.sh`, `dacp.sh`, `dacp-proxy.sh`)
- Assemble modules, load config, and build distributions
- Unify deployment entry across test/dev/prod

Key interfaces: `ServerStart`, `FileDirectoryDataSourceModule`, assembly descriptors  
Primary dependencies: `packaging`, runtime modules

## 3. Protocol Design

### 3.1 Core Concepts

- **SDF (Streaming DataFrame)**: schema-aware 2D mini-table as streaming unit.
- **DACP URL/resource identity**: unified identifier for re-fetching and reproducibility.
- **Ticket**: decouples control-plane response from data-plane transfer.

### 3.2 Unified Interaction Model (Control Plane + Data Plane)

- **Action (control plane)**: JSON request, JSON or `ticket + metadata` response.
- **GetStream (data-plane read)**: pull Arrow Flight stream by ticket.
- **PutStream (data-plane write)**: push Arrow Flight stream by ticket.

Benefits:

- lightweight control plane and easy routing
- lazy/interruptible data consumption
- proxy-level ticket re-registration for topology hiding

### 3.3 Authentication

Flow:

1. Client authenticates with credentials.
2. Server selects matching `AuthenticationMethod` and emits `UserPrincipal`.
3. Session token maps to `UserPrincipal`.
4. Subsequent GET/PUT/Action use this identity for auth and audit.

### 3.4 GET: Forward Provisioning (Fetch Data)

Semantics: client declares what to fetch; server streams transformed data as SDF.

Implementation path:

1. Client builds `TransformOp`.
2. Client invokes GET action.
3. Server executes transform tree.
4. Server registers result as ticket and returns metadata + ticket.
5. Client streams by ticket and consumes lazily.

### 3.5 PUT: Reverse of Forward Provisioning (Upload Data/Version)

Semantics: push local/intermediate data to data side and receive re-fetchable identifier.

Implementation path:

1. Action obtains upload ticket.
2. `PutStream` pushes RecordBatch stream.
3. Server persists via `PutStreamMethod`.

### 3.6 COOK: Reverse Provisioning (Send Algorithms to Data)

Semantics: move algorithms to data-side execution and return only results.

#### 3.6.1 Recipe (Conceptual Model)

Recommended fields:

- `code`: inline code (Python/R/Julia/SQL) or serialized operator expression (mutually exclusive with `operatorRef`)
- `operatorRef`: repository operator reference (`operatorId`, `operatorVersion`, `entrypoint`, `artifactType`)
- `operatorArgs`: operator arguments (must satisfy operator parameter schema)
- `env`: runtime environment (Conda/Docker/WASM)
- `resource`: CPU/memory/GPU quotas and constraints
- `outputSchema`: pre-declared output schema
- `params`: runtime parameters
- `token`: one-time, least-privilege access token

Invocation constraints:

- Inline mode: provide `code` without `operatorRef`
- Repository mode: provide `operatorRef` (optional `operatorArgs`), `code` can be empty
- If both exist, prioritize `operatorRef` and emit validation warning

#### 3.6.2 COOK Output

COOK should return compact outputs first:

- result tables (SDF)
- aggregates/statistics
- extracted features

#### 3.6.3 Integration Points with Operator Repository

1. Control plane (submission):
   - `SUBMIT_RECIPE`/`SUBMIT_FLOW` support `operatorId`, `operatorVersion`, `entrypoint`, `params`.
   - Validate existence, version, and parameter schema before enqueueing.

2. Execution plane (scheduling):
   - Scheduler uses `RepositoryClient` to pull operator descriptors/artifacts.
   - `FlowExecutionContext` injects operator definition, runtime env, and data token.
   - Multi-node flow passes references and intermediate handles, not large artifacts.

3. Result plane (return):
   - Normalize to `DataFrame`/`Blob`, register ticket, stream back to clients.
   - Persist `operatorId/operatorVersion` for reproducibility.

Contract recommendations:

- version policy: fixed version/range/latest (fixed preferred in production)
- schema validation: fail fast at submission
- artifact cache: local cache by version hash with TTL
- security boundary: repository whitelist and separated pull/execute credentials

#### 3.6.4 Mapping to Current Implementation

Current `cook-module` actions:

- `SUBMIT_RECIPE`
- `SUBMIT_FLOW`
- `GET_JOB_STATUS`
- `GET_JOB_EXECUTE_PROCESS`
- `GET_JOB_EXECUTE_RESULT`

Conclusion: COOK reuses DFTP ticket+stream as the result return channel.

## 4. Extension Design

### 4.1 Catalog Extension (Directory/Metadata)

- SPI: `CatalogService`
- Module: `catalog-module`
- Extensible aspects: source backends, RDF/JSON-LD vocabularies, schema/stats/doc strategy, versioning/lineage policy

### 4.2 Cook Extension (Executors/Operators)

- Extensible aspects: `TransformOp`/Flow node types, repository operators, multi-language executors, runtime env management, job governance

### 4.3 Proxy Extension (Protocol-Level Proxy)

- Extensible aspects: forwarding strategy, ticket re-registration policy, internal connection governance

## 5. Data Model Design

### 5.1 DataFrame / Row / StructType

- `StructType`: schema definition
- `Row`: row-level value container
- `DataFrame`: streaming tabular abstraction

Arrow mapping:

- schema -> Arrow Schema
- data -> Arrow RecordBatch
- batch/frame size as key tuning knobs

### 5.2 Blob

- `Blob` for binary streams
- can be carried as single-bytes-column DataFrame stream
- metadata should include size and MIME/custom type

### 5.3 TransformOp / Flow / Job

- `TransformOp`: source + transform tree
- `Flow`: DAG/multi-output execution graph
- `Job`: operational entity (`jobId`, status, progress, throughput, results)

## 6. Security Design

### 6.1 Authentication and Identity

- Authentication is implemented via pluggable `AuthenticationMethod`.
- Credentials are mapped to `UserPrincipal` for authorization, proxy propagation, and audit.

### 6.2 Authorization and Least Privilege (for COOK)

- one-time token for scoped resource access
- operation-level permission checks
- multi-tenant isolation across jobs and data access

### 6.3 Isolation and Sandbox (Planned)

Current status: **Not implemented yet** (targeting M3/M4).

Planned directions:

- sandbox container (read-only FS + restricted network)
- resource quotas and execution timeout
- dependency/image whitelist and cache reuse
- token-scoped data access boundary

### 6.4 Auditing and Observability

Planned dedicated module: `AccessLogModule` as a `DftpModule` plugin for unified access logging.

Goals:

- structured access logs for GET/PUT/COOK/Catalog/Proxy
- non-invasive and configurable deployment
- compatible with log4j2 and external analytics systems

Recommended logging points:

- Action request in/out (with latency)
- stream start/end/error for GetStream/PutStream
- job submit/status transition/result retrieval

Recommended log dimensions:

- who / what / when / where / how much

Implementation constraints (phase 1):

- mandatory `traceId` for linking control and data planes
- sensitive-data masking (password/token/key materials)
- asynchronous logging under high concurrency

## 7. Extensibility Design

### 7.1 Modular Extension Mechanism (System Core)

Extensibility is a core highlight. The system uses a three-part model:

- module registration (`DftpModule` + `init(anchor, serverContext)`)
- extension-point collection (`EventHandler` / `EventSource`)
- runtime routing (`KernelModule` routes Authentication/Action/GetStream/PutStream)

Boundary:

- `DftpServer`: lifecycle and container context
- `KernelModule`: routing and aggregation
- business modules: capability implementation only

### 7.2 Extension Points (Implementation Guidance)

1. Authentication extensions (`AuthenticationMethod`)
2. Control-plane extensions (`ActionMethod`)
3. Data-plane extensions (`GetStreamMethod`, `PutStreamMethod`)
4. Execution/catalog extensions (`CatalogService`, TransformOp/Flow, repository integration)
5. Platform extensions (proxy strategy, `AccessLogModule`)

### 7.3 Extension Lifecycle and Assembly Flow

1. design contract
2. implement module/handlers with unit tests
3. assemble in `ServerStart` or deployment config
4. initialize via `init(...)`
5. validate via logs/metrics

Runtime constraints:

- initialization must be idempotent
- fail fast on init failure with clear diagnostics
- no uncontrolled runtime mutation across modules

### 7.4 Extension Priority and Conflict Handling

- auth chain: first accepted handler wins
- action routing: exact action-type match with priority-based conflict resolution
- stream routing: match by resource/protocol type

Conflict governance:

- priority config (`module.priority.<name>`)
- startup conflict detection
- audit logs for override behavior

### 7.5 Stability Boundaries and Compatibility Constraints

- stable API surface: `DftpModule`, method interfaces, `ServerContext` core capability
- evolvable surface: action names, module internals, strategy params
- forbidden: cross-module private-state access and bypassing `KernelModule`

Compatibility:

- additive backward-compatible fields
- no semantic reuse of existing error codes
- explicit release notes for behavior changes

### 7.6 Extension Implementation Template (Engineering Convention)

Each new module should define:

1. module definition and boundaries
2. API contract and error codes
3. lifecycle hooks
4. configuration schema
5. test plan
6. observability plan
7. security controls

## 8. Engineering Implementation View

### 8.1 Module Dependency Relationship (Maven)

- `dftp-common` as shared protocol/model core
- `dftp-server` and `dftp-client` over `dftp-common`
- `catalog-module` and `cook-module` over server/common
- `dacp-client` over client/common
- `dacp-proxy` over server/client/common
- `packaging` as assembly/distribution

Constraints:

- keep shared models in `dftp-common`
- no reverse dependency from business modules to `packaging`
- keep `dacp-client` server-independent

### 8.2 Key Sequences (Implementation Constraints)

GET:
1) build `TransformOp`  
2) action call  
3) execute and register ticket  
4) stream by ticket

PUT:
1) get upload ticket  
2) stream upload  
3) persist

COOK:
1) submit recipe/flow  
2) create/schedule job  
3) poll status/progress  
4) get result ticket and stream

Notes:

- ticket lifecycle bound to `ServerContext`
- large results must return by ticket, not inline payload
- proxy re-registers result tickets

### 8.3 Configuration Matrix (Phase 1)

Configuration groups in `dftp.conf`/`dacp.conf`:

- network (`*.host.position`, `*.host.port`, `*.scheme`)
- module assembly (XML dynamic registration via `dftp.xml`/`dacp.xml`)
- auth (anonymous switch, credential source, key path)
- ticket (TTL, max cache size, cleanup interval)
- data source (root dir, allowed formats, file-size limit)
- cook (job concurrency, timeout, temp dir, container switch)
- proxy (upstream, pool, timeout, retry)
- observability (log level, audit switch, metrics interval)

XML dynamic module registration minimum fields:

- `module.class` (required)
- `module.enabled` (default `true`)
- `module.order` (default `0`, lower first)
- `module.configRef` (optional)

Loading constraints:

- phase 1: startup-time load only (no hot plug)
- missing class/init failure causes startup failure
- handler conflicts resolved by order/priority rule

### 8.4 Exception and Error Semantics (Recommended)

Unified error model: `code` + `message` + `traceId` + `details`.

Error domains:

- `AUTH_*`, `PERM_*`, `REQ_*`, `DATA_*`, `JOB_*`, `SYS_*`

Status mapping (business code -> HTTP semantics -> Arrow Flight status):

| Business Code | HTTP Semantics | Arrow Flight Status (Recommended) | Typical Scenario |
|---|---:|---|---|
| `AUTH_INVALID_CREDENTIALS` | 401 | `UNAUTHENTICATED` | invalid login credentials |
| `AUTH_MISSING_CREDENTIALS` | 401 | `UNAUTHENTICATED` | missing credentials |
| `PERM_ACCESS_DENIED` | 403 | `UNAUTHORIZED` | authenticated but forbidden |
| `DATA_NOT_FOUND` | 404 | `NOT_FOUND` | missing dataset/resource/ticket |
| `JOB_NOT_FOUND` | 404 | `NOT_FOUND` | missing `jobId` |
| `REQ_INVALID_ARGUMENT` | 400 | `INVALID_ARGUMENT` | bad parameters/schema mismatch |
| `REQ_UNSUPPORTED_OPERATION` | 400 | `INVALID_ARGUMENT` | unsupported action/arg |
| `DATA_ALREADY_EXISTS` | 409 | `ALREADY_EXISTS` | PUT version conflict |
| `JOB_CONFLICT` | 409 | `FAILED_PRECONDITION` | invalid job-state transition |
| `JOB_TIMEOUT` | 408 | `TIMED_OUT` | job timeout |
| `SYS_BACKEND_UNAVAILABLE` | 503 | `UNAVAILABLE` | upstream dependency unavailable |
| `SYS_INTERNAL_ERROR` | 500 | `INTERNAL` | uncategorized internal error |

Rules:

- external docs can describe HTTP semantics; Flight layer returns matching `CallStatus`
- stable `code`, readable `message`, machine-parseable `details`
- unmapped exceptions default to `SYS_INTERNAL_ERROR -> INTERNAL`
- proxy should preserve downstream `traceId` and append `proxyTraceId`

Minimum requirements:

- full stack trace in server logs
- only necessary error info in responses
- `traceId` in all action responses

### 8.5 Test Strategy (Aligned by Module)

- `dftp-common`: model/codec/type tests
- `dftp-client`/`dftp-server`: GET/PUT/auth/recovery integration tests
- `catalog-module`: catalog contract and metadata format tests
- `cook-module`: scheduler/state transitions/executor isolation tests
- `dacp-proxy`: forwarding and ticket re-registration tests
- `packaging`: startup script/config smoke tests

CI gates:

- all unit tests pass
- at least one E2E path (GET + COOK + Proxy)
- coverage threshold (phase 1: 60%, then raise)

## 9. Summary

DFTP-DACP delivers:

- high-performance streaming 2D data frames
- control/data-plane decoupling with ticket-based transfer
- modular extensibility for protocol, execution, and platform capabilities

It supports both forward provisioning (GET) and reverse provisioning (COOK), and can be assembled per scenario through Catalog/Cook/Proxy modules while evolving security and observability over time.
