# DFTP-DACP Development Document

[中文](./DFTP-DACP-Development-Document-ZH.md) | [English](./DFTP-DACP-Development-Document.md)

## Table of Contents

- [1. Document Overview](#1-document-overview)
  - [1.1 Purpose](#11-purpose)
  - [1.2 Audience](#12-audience)
  - [1.3 Relationship with the Design Document](#13-relationship-with-the-design-document)
- [2. Project Structure](#2-project-structure)
  - [2.1 Maven Modules](#21-maven-modules)
  - [2.2 Dependency Direction](#22-dependency-direction)
  - [2.3 Engineering Boundaries](#23-engineering-boundaries)
- [3. Local Development Environment](#3-local-development-environment)
  - [3.1 Requirements](#31-requirements)
  - [3.2 Build and Test](#32-build-and-test)
  - [3.3 Start Services](#33-start-services)
- [4. Core Runtime Mechanism](#4-core-runtime-mechanism)
  - [4.1 DftpServer Lifecycle](#41-dftpserver-lifecycle)
  - [4.2 KernelModule Routing](#42-kernelmodule-routing)
  - [4.3 Control Plane and Data Plane Separation](#43-control-plane-and-data-plane-separation)
  - [4.4 Ticket Lifecycle](#44-ticket-lifecycle)
- [5. Module Extension Development Guide](#5-module-extension-development-guide)
  - [5.1 Module Extension Model](#51-module-extension-model)
  - [5.2 DftpModule Interface Contract](#52-dftpmodule-interface-contract)
  - [5.3 Module Lifecycle](#53-module-lifecycle)
  - [5.4 Module Assembly](#54-module-assembly)
  - [5.5 Module Loading Order and Conflict Handling](#55-module-loading-order-and-conflict-handling)
  - [5.6 Module Initialization Constraints](#56-module-initialization-constraints)
  - [5.7 Module Configuration Rules](#57-module-configuration-rules)
  - [5.8 Module Resource Management](#58-module-resource-management)
  - [5.9 Module Logging and Observability](#59-module-logging-and-observability)
  - [5.10 Module Testing Requirements](#510-module-testing-requirements)
- [6. Extension Point Development Guide](#6-extension-point-development-guide)
  - [6.1 Authentication Extension](#61-authentication-extension)
  - [6.2 Action Extension](#62-action-extension)
  - [6.3 GetStream Extension](#63-getstream-extension)
  - [6.4 PutStream Extension](#64-putstream-extension)
  - [6.5 Catalog Extension](#65-catalog-extension)
  - [6.6 Cook Extension](#66-cook-extension)
  - [6.7 Proxy Extension](#67-proxy-extension)
- [7. Complete Example: Developing a New Module](#7-complete-example-developing-a-new-module)
- [8. Client Development Guide](#8-client-development-guide)
- [9. Configuration Development Guide](#9-configuration-development-guide)
- [10. Error Codes and Exception Handling](#10-error-codes-and-exception-handling)
- [11. Testing Guide](#11-testing-guide)
- [12. Packaging and Deployment](#12-packaging-and-deployment)
- [13. Observability and Troubleshooting](#13-observability-and-troubleshooting)
- [14. Development Conventions](#14-development-conventions)
- [15. Appendix](#15-appendix)

## 1. Document Overview

### 1.1 Purpose

This document describes how to develop, extend, test, package, deploy, and troubleshoot DFTP-DACP.

It focuses on:

- understanding the project structure and module boundaries;
- building, testing, and starting DFTP/DACP services locally;
- developing new `DftpModule` implementations;
- extending authentication, control-plane Action, data-plane GetStream, and data-plane PutStream;
- extending DACP capabilities such as Catalog, Cook, and Proxy;
- assembling modules through configuration and validating them in tests.

The core of this document is **Module extension development**. DFTP-DACP should not grow business capabilities by modifying the protocol kernel directly. New capabilities should be added through modules, collected through cross-module events, and routed by `KernelModule`.

### 1.2 Audience

This document is intended for:

- protocol kernel developers;
- server-side module developers;
- data source integration developers;
- Catalog / Cook / Proxy extension developers;
- client SDK developers;
- packaging, deployment, and operations engineers.

### 1.3 Relationship with the Design Document

The design document explains why the system is designed this way. This development document explains how developers should implement, extend, and deliver the system.

Read this document together with the [DFTP-DACP Design Document](./DFTP-DACP-Design-Document.md). The architecture, protocol, data model, security model, and extension design described there are the basis for the development constraints in this document.

## 2. Project Structure

### 2.1 Maven Modules

The project should be understood by module responsibility:

- `dftp-common`: common protocol models and abstractions, including `DataFrame`, `Row`, `StructType`, `Blob`, `TransformOp`, authentication models, codecs, and utility classes.
- `dftp-server`: server-side protocol kernel, including `DftpServer`, `ServerContext`, `KernelModule`, module interfaces, and the four core extension points.
- `dftp-client`: base client layer, wrapping Flight connections, authentication, Action calls, GET stream access, and PUT stream access.
- `catalog-module`: DACP Catalog capability module, providing dataset/dataframe discovery, metadata query, schema, statistics, and documentation capabilities.
- `cook-module`: DACP Cook capability module, providing Recipe/Flow submission, job scheduling, status query, progress query, and result retrieval.
- `dacp-client`: high-level DACP client API for Catalog and Cook operations.
- `dacp-proxy`: DACP protocol-level proxy, providing backend forwarding, result Action re-wrapping, and ticket re-registration.
- `packaging`: distribution assembly module, providing startup scripts, configuration templates, and deployable packages.

### 2.2 Dependency Direction

Dependencies must remain clear and acyclic:

- `dftp-common` does not depend on business modules.
- `dftp-server` depends on `dftp-common`.
- `dftp-client` depends on `dftp-common`.
- Business modules depend on `dftp-server + dftp-common`.
- `dacp-client` depends on `dftp-client + dftp-common`.
- `dacp-proxy` depends on `dftp-server + dftp-client + dftp-common`.
- `packaging` aggregates artifacts but must not contain protocol kernel or business logic.

Constraints:

- Shared data models must live in `dftp-common`.
- Business modules must not depend back on `packaging`.
- Client modules must not depend on server-side modules.
- Modules must not call each other through private implementations. They should interact through public interfaces, cross-module events, or protocol requests.

### 2.3 Engineering Boundaries

The following boundaries must be maintained:

- Protocol kernel boundary: `DftpServer` and `KernelModule` manage lifecycle, context, handler collection, and routing. They should not contain business behavior.
- Business module boundary: Catalog, Cook, Proxy, AccessLog, and similar modules implement their own capabilities and attach through extension points.
- Common model boundary: shared data structures, error models, and protocol DTOs belong in the common layer.
- Client boundary: clients call remote services and must not reference server-side implementations.
- Deployment boundary: `packaging` only assembles configuration, scripts, and distribution packages.

## 3. Local Development Environment

### 3.1 Requirements

Recommended development environment:

- JDK: consistent with the project `pom.xml`;
- Maven: 3.8+ is recommended;
- IDE: IntelliJ IDEA or another IDE with Maven multi-module support;
- OS: macOS, Linux, or Windows for development; Linux is recommended for production;
- Network access: the first build needs access to Maven repositories.

Check the environment before development:

```bash
java -version
mvn -version
```

### 3.2 Build and Test

Build all modules:

```bash
mvn clean package
```

Run unit tests:

```bash
mvn test
```

Package while skipping tests:

```bash
mvn clean package -DskipTests
```

Build a specific module and its dependencies:

```bash
mvn -pl <module-name> -am package
```

Baseline development requirements:

- New common models must have unit tests.
- New modules must have assembly tests.
- New Actions must have request validation tests.
- New Stream handlers must have resource closing and exception-path tests.

### 3.3 Start Services

Services are usually started through scripts or entry points provided by the `packaging` module.

Typical service modes:

- DFTP Server: assembles only the base protocol kernel and data source modules.
- DACP Server: assembles the DFTP kernel plus upper-layer capabilities such as Catalog and Cook.
- DACP Proxy: assembles the DFTP kernel, Proxy forwarding module, and client-side backend connection capability.

Before starting a service, verify:

- host / port are configured correctly;
- module XML or module list is correct;
- data source root directory or backend address is accessible;
- ticket TTL and cleanup parameters match the test scenario;
- log directory is writable.

## 4. Core Runtime Mechanism

### 4.1 DftpServer Lifecycle

`DftpServer` is the server container. A typical lifecycle is:

1. Load server configuration.
2. Create `ServerContext`.
3. Initialize `KernelModule`.
4. Load and initialize business modules.
5. Collect Authentication / Action / GetStream / PutStream handlers.
6. Start Arrow Flight Server.
7. Accept authentication, Action, GetStream, and PutStream requests.
8. Release tickets, thread pools, connection pools, temporary directories, and other resources when shutting down.

Development constraints:

- `DftpServer` should not directly depend on Catalog/Cook/Proxy business implementations.
- Startup failures must produce clear errors.
- Module initialization failures should fail fast by default to avoid a partially available service.
- Service shutdown must have a unified cleanup path.

### 4.2 KernelModule Routing

`KernelModule` is the protocol routing kernel. It aggregates and routes four handler types:

- `AuthenticationMethod`: handles authentication.
- `ActionMethod`: handles control-plane requests.
- `GetStreamMethod`: handles data-plane reads.
- `PutStreamMethod`: handles data-plane writes.

Handlers are provided by modules. `KernelModule` does not know business semantics. It matches and dispatches protocol requests.

Routing requirements:

- The authentication chain should have deterministic order.
- Action routing should match by `actionType` precisely.
- GetStream should locate registered data streams by ticket precisely.
- PutStream first restores upload parameters by upload ticket in `acceptPut`, then selects a handler through `PutStreamMethod.accepts(request)`.
- Missing handlers should return standardized errors.
- Handler conflicts should be detected or warned about during startup where possible.

### 4.3 Control Plane and Data Plane Separation

DFTP-DACP separates the control plane and the data plane:

- Control plane: Action requests for parameter negotiation, permission checks, job submission, and ticket registration.
- Data plane: GetStream / PutStream requests for Arrow RecordBatch or Blob chunk transfer.

Development requirements:

- Actions must not directly return large data.
- Large results must be registered as tickets.
- Clients pull or push streams through tickets.
- Proxy must re-register tickets for result-producing requests.
- `traceId` chain tracing is a planned capability. Once implemented, it should cover both control-plane and data-plane paths for cross-link troubleshooting.

### 4.4 Ticket Lifecycle

A ticket bridges the control plane and the data plane.

Typical flow:

1. An Action creates or obtains a `DataFrameHandle` / `BlobHandle`.
2. The server registers `ticket -> handle`.
3. The Action returns ticket and metadata.
4. The client calls GetStream / PutStream with the ticket.
5. The server locates the handle by ticket.
6. Resources are cleaned up when the stream finishes, expires, or fails.

Development requirements:

- Tickets must have TTL.
- Missing tickets should return `DATA_NOT_FOUND` or an equivalent error.
- Tickets must not leak backend internal addresses.
- Proxy must not expose backend tickets to external clients.
- Ticket cleanup must not block the data-plane main path.
- Ticket logs should reserve a chain-tracing field. Once traceId is implemented, it should be written to that field.

## 5. Module Extension Development Guide

### 5.1 Module Extension Model

Module is the core extension unit of DFTP-DACP. New capabilities should be implemented as modules instead of modifications to the protocol kernel.

The module extension model has three parts:

- `DftpModule`: module entry point, responsible for initialization and extension registration.
- `EventHandler` / `EventSource`: cross-module event mechanism used to collect extension points.
- `KernelModule`: collects four handler families and routes requests.

Development principles:

- Keep the kernel stable and capabilities pluggable.
- A module declares and implements only its own capability.
- Modules interact through interfaces, events, or protocols.
- Modules must not directly rewrite kernel routing semantics.
- Business modules must not bypass `KernelModule` to handle protocol requests directly.

### 5.2 DftpModule Interface Contract

Each module implements `DftpModule` and attaches `EventHandler` / `EventSource` through `Anchor` during initialization. In the current implementation, modules do not directly register `ActionMethod`, `GetStreamMethod`, or other handlers into `Anchor`. They register capabilities through cross-module collection events.

Example:

```scala
class XxxActionModule extends DftpModule {
  private var serverContext: ServerContext = _

  private val actionMethod = new ActionMethod {
    override def accepts(request: DftpActionRequest): Boolean = {
      request.getActionName() == "XXX_ACTION"
    }

    override def doAction(request: DftpActionRequest, response: DftpActionResponse): Unit = {
      response.sendJSONString("{}")
    }
  }

  private val eventHandler = new EventHandler {
    override def accepts(event: CrossModuleEvent): Boolean = {
      event.isInstanceOf[CollectActionMethodEvent]
    }

    override def doHandleEvent(event: CrossModuleEvent): Unit = {
      event match {
        case r: CollectActionMethodEvent => r.collect(actionMethod)
        case _ =>
      }
    }
  }

  override def init(anchor: Anchor, serverContext: ServerContext): Unit = {
    this.serverContext = serverContext
    anchor.hook(eventHandler)
  }

  override def destroy(): Unit = {
  }
}
```

If a module also needs to collect capabilities exposed by other modules, it may attach an `EventSource`. `Modules` initializes all attached event sources after all modules have run `init`:

```scala
anchor.hook(new EventSource {
  override def init(eventHub: EventHub): Unit = {
    eventHub.fireEvent(CollectGetStreamMethodEvent(getMethods))
  }
})
```

Module initialization may include:

- saving a `ServerContext` reference;
- creating the module's `AuthenticationMethod` / `ActionMethod` / `GetStreamMethod` / `PutStreamMethod`;
- attaching an event handler through `anchor.hook(eventHandler)`;
- attaching an event source through `anchor.hook(eventSource)` if the module needs to actively collect capabilities from other modules;
- reading and validating required configuration if the module depends on configuration;
- initializing services, connection pools, thread pools, or caches if the module depends on external resources;
- writing module loading logs.

Built-in collection events:

- `CollectAuthenticationMethodEvent`: collects `AuthenticationMethod`.
- `CollectActionMethodEvent`: collects `ActionMethod`.
- `CollectPutStreamMethodEvent`: collects `PutStreamMethod`.
- `CollectGetStreamMethodEvent`: collects `GetStreamMethod` and `GetStreamFilter`.

Collection flow:

1. All modules execute `init(anchor, serverContext)`.
2. Modules attach their own `EventHandler` and `EventSource` through `anchor.hook(...)`.
3. After all module initialization finishes, `Modules` initializes all attached `EventSource` instances.
4. `EventSource` emits collection events through `EventHub.fireEvent(...)`.
5. Matching `EventHandler` instances add their capabilities to the collector carried by the event.
6. `KernelModule` uses the collected handlers to route Authentication, Action, GetStream, and PutStream.

Interface constraints:

- `init` must be idempotent.
- `init` should not run long blocking tasks.
- `init` failures must throw clear exceptions.
- A module must not swallow initialization errors and continue running.
- A module should avoid complex initialization in the constructor.
- `EventHandler.accepts` should match event types precisely to avoid handling irrelevant events.
- `ActionMethod.accepts` should not unconditionally return `true` unless it is a fallback handler, because it may preempt later Action handlers.

### 5.3 Module Lifecycle

Lifecycle of a new module:

1. Design module responsibility.
2. Define input/output contracts.
3. Implement `DftpModule`.
4. Implement corresponding handlers.
5. Attach `EventHandler` / `EventSource` through `Anchor` in `init`.
6. Add configuration items.
7. Add XML or startup assembly configuration.
8. Write unit tests.
9. Write module assembly tests.
10. Start the service and verify behavior.
11. Add logs, metrics, and troubleshooting notes.
12. Include the module in the distribution package.

During shutdown, a module should release:

- thread pools;
- connection pools;
- file handles;
- temporary directories;
- local caches;
- backend client connections;
- resources associated with unfinished tickets.

### 5.4 Module Assembly

The current implementation assembles modules through the Spring XML `modules` list. Modules are created as Spring `DftpModule` beans and injected into `DftpServerConfigBean.modules`.

Currently implemented fields:

- `class`: module implementation class, declared by the Spring bean `class` attribute.
- constructor arguments: injected through `<constructor-arg>`.
- Java/Scala bean properties: injected through `<property>`.

Module metadata not yet implemented:

- `module.enabled`: module-level enable/disable is not uniformly handled by the framework.
- `module.order`: explicit order metadata is not implemented.
- `module.configRef`: configuration prefix reference is not implemented.

The current module loading order is determined by the bean declaration order in the XML `<list>`.

Example:

```xml
<property name="modules">
    <list>
        <bean class="link.rdcn.server.module.BaseDftpModule"/>
        <bean class="link.rdcn.server.module.UserPasswordAuthModule">
            <constructor-arg>
                <bean class="link.rdcn.server.module.DefaultUserPasswordAuthService"/>
            </constructor-arg>
        </bean>
        <bean class="link.rdcn.dacp.catalog.DacpCatalogModule"/>
        <bean class="link.rdcn.server.FileDirectoryDataSourceModule">
            <property name="rootDirectory">
                <value type="java.io.File">file:${configDir}/../data</value>
            </property>
        </bean>
    </list>
</property>
```

Assembly requirements:

- Startup fails if a class does not exist.
- Startup fails if module construction fails.
- Startup fails if module initialization fails.
- To disable a module currently, remove or comment out the corresponding bean from the XML `modules` list.
- If a module implements its own `enabled` configuration, it should check that value in `init` and avoid attaching handlers when disabled.
- Module loading order should follow XML list order.

### 5.5 Module Loading Order and Conflict Handling

Current loading order:

- `DftpServer` creates the internal `KernelModule`.
- Modules declared in XML are added to `Modules` in `modules` list order.
- During `buildServer()`, the internal `KernelModule` is appended, then `modules.init()` is executed.
- `modules.init()` first calls `init(anchor, serverContext)` on each module in insertion order, then initializes all attached `EventSource` instances in hook order.

Recommended XML declaration order:

1. Base kernel module.
2. Authentication module.
3. Data source module.
4. Catalog module.
5. Cook module.
6. Proxy module.
7. Observability modules such as AccessLog / Metrics.

Conflict handling rules:

- Authentication: match by registration order or explicit priority; the first handler that accepts the credentials is used.
- Action: match precisely by actionType; duplicate registration should warn or fail during startup.
- GetStream: match registered DataFrame / Blob streams precisely by ticket.
- PutStream: upload ticket is used only in `acceptPut` to restore upload parameters; after entering `KernelModule`, `PutStreamMethod.accepts(request)` selects the handler.
- Proxy: re-wrapping Actions should clearly declare override strategy.

Recommended baseline strategy:

- The same actionType should not be registered by multiple modules.
- The same ticket should not be handled by multiple Stream handlers.
- Conflict lists should be printed in startup logs.
- Production deployments should fail fast on severe conflicts by default.

Planned capabilities:

- explicit `module.order`;
- unified `module.enabled`;
- startup conflict detection;
- module configuration prefix `module.configRef`.

### 5.6 Module Initialization Constraints

Module initialization must be:

- idempotent: repeated initialization must not duplicate handler registration;
- diagnosable: failures must include module name, configuration prefix, and root cause;
- controlled: initialization must not start infinite loops or long blocking tasks;
- isolated: a module must not directly mutate private state of another module;
- least-privilege: a module should only read its own configuration and necessary context;
- cleanable: if initialization partially succeeds and then fails, created resources must be released.

Forbidden behavior:

- bypassing `KernelModule` to handle Flight requests directly;
- force-casting across modules to access private implementations;
- storing user credentials in static global caches;
- logging passwords, tokens, or private keys;
- returning large data directly from Action.

### 5.7 Module Configuration Rules

Configuration keys should use module prefixes:

```text
dftp.*
dacp.*
catalog.*
cook.*
proxy.*
ticket.*
accesslog.*
```

Each module should document its configuration matrix:

| Key | Type | Default | Required | Description |
|---|---|---:|---|---|
| `xxx.enabled` | boolean | `true` | No | Whether the module is enabled |
| `xxx.timeoutMs` | long | `30000` | No | Request timeout |
| `xxx.rootDir` | string | none | Yes | Module data root directory |

Configuration reading requirements:

- Missing required fields should fail startup.
- Invalid values should fail during startup.
- Default values should be declared explicitly.
- Sensitive values must not be printed in plaintext.
- Configuration changes that affect behavior must be documented.

### 5.8 Module Resource Management

If a module owns external resources, it must define cleanup strategy.

Common resources:

- database connection pools;
- object storage clients;
- remote DFTP/DACP clients;
- thread pools;
- local caches;
- temporary files;
- executor working directories;
- DataFrame / Blob handles associated with tickets.

Development requirements:

- Resource creation and release paths must be paired.
- Underlying readers must be closed when a Stream read is interrupted.
- Temporary files must be cleaned when PutStream upload fails.
- Cook execution resources must be released when a job fails.
- Proxy backend connections should have timeout, retry, and pool-size limits.
- Cleanup failures should be logged, but should not stop the rest of the cleanup flow.

### 5.9 Module Logging and Observability

Module logs should cover:

- module loading;
- configuration summary;
- handler registration;
- Action request entry and return;
- Stream start and end;
- ticket registration and cleanup;
- job status changes;
- external dependency calls;
- exceptions and timeouts.

Logging requirements:

- The current implementation does not provide a unified `traceId` mechanism.
- Planned request chains should carry `traceId`.
- Once traceId is implemented, Action and Stream logs should be correlatable by traceId.
- Sensitive information must be masked.
- Error logs should keep server-side stack traces.
- External responses should only expose necessary error information.

Suggested metrics:

- Action request count, failure count, latency;
- active Stream count, throughput, bytes;
- ticket count, expired ticket count;
- Cook job count, failure count, average duration;
- Proxy backend request count, retry count, timeout count;
- module initialization duration.

### 5.10 Module Testing Requirements

Each new module should include:

- module initialization tests;
- missing configuration tests;
- invalid configuration tests;
- handler registration tests;
- routing tests with `KernelModule`;
- successful request tests;
- exceptional request tests;
- resource release tests.

Module testing checklist:

- Module can be assembled by XML.
- Disabled modules do not register handlers if they implement an enabled switch.
- actionType does not conflict.
- Tickets can be registered, queried, and expired.
- Resources are closed after Stream interruption.
- Error codes comply with conventions.
- If the module implements traceId, logs include traceId.
- Sensitive fields do not enter logs.

## 6. Extension Point Development Guide

### 6.1 Authentication Extension

Authentication extension adds new authentication methods.

Use cases:

- anonymous authentication;
- username/password authentication;
- token authentication;
- keypair authentication;
- external IdP mapping.

Implementation requirements:

- Implement `AuthenticationMethod`.
- Decide whether the method supports the current credentials.
- Validate credentials.
- Create `UserPrincipal`.
- Inject identity into downstream request context.
- Return standardized `AUTH_*` errors on authentication failures.

Flow:

1. Client submits credentials.
2. `KernelModule` iterates authentication handlers.
3. A matching handler validates credentials.
4. On success, user identity is returned.
5. Later Action / Stream requests use the identity for authorization and audit.

Development notes:

- Multiple authentication modules must have clear priority.
- Tokens, passwords, and private keys must not be logged.
- Authentication modules should avoid dependencies on business modules.
- External IdP failures should return diagnosable errors.

### 6.2 Action Extension

Action extension adds new control-plane capabilities.

Use cases:

- GET parameter negotiation;
- PUT upload negotiation;
- Catalog query;
- Cook job submission;
- Job status query;
- Proxy forwarding;
- administrative requests.

Implementation requirements:

- Implement `ActionMethod`.
- Define a unique actionType.
- Validate request parameters.
- Perform permission checks.
- Return JSON or ticket metadata.
- Return unified error models on failure.

Recommended actionType naming:

```text
CATALOG_LIST_DATASETS
CATALOG_GET_SCHEMA
COOK_SUBMIT_RECIPE
COOK_SUBMIT_FLOW
COOK_GET_JOB_STATUS
COOK_GET_JOB_EXECUTE_RESULT
PROXY_FORWARD_ACTION
```

Action response requirements:

- Small results may be returned as JSON.
- Large results must be registered as tickets.
- traceId is a planned capability. Once implemented, Action responses should include traceId.
- Error responses must include stable error codes.
- Responses must not expose server internal paths, backend tickets, or sensitive configuration.

### 6.3 GetStream Extension

GetStream extension adds data read capability.

Use cases:

- reading local files;
- reading object storage;
- reading databases;
- reading cache;
- reading Cook results;
- reading backend results through Proxy.

Implementation requirements:

- Implement `GetStreamMethod`.
- Locate the registered data stream precisely by ticket.
- Stream Arrow RecordBatch or equivalent chunks.
- Support early client interruption.
- Close underlying resources at the end.
- Return standardized errors on exceptions.

Development notes:

- Do not load large data all at once.
- RecordBatch size should be configurable.
- Data source readers must be released in `finally` or an equivalent path.
- Missing tickets should return `DATA_NOT_FOUND`.
- Permission failures should return `PERM_ACCESS_DENIED`.
- Blob can reuse the unified stream mechanism.

### 6.4 PutStream Extension

PutStream extension adds data write capability.

Use cases:

- uploading DataFrame;
- uploading Blob;
- writing to filesystem;
- writing to object storage;
- writing to database;
- writing data versions managed by Catalog.

Typical flow:

1. Client starts an Action for upload negotiation.
2. Server validates target, permissions, schema, and size limits.
3. Server creates an upload ticket.
4. Client pushes data through PutStream.
5. Server consumes the stream and writes to target storage.
6. Server returns a resource identifier or version information.

Development requirements:

- Validate parameters before upload.
- Support interruption during upload.
- Clean temporary data on upload failure.
- Return `DATA_ALREADY_EXISTS` or a version conflict error when the target already exists.
- Produce a retrievable identifier after successful write.
- Keep the boundary clear between PUT and Catalog version management.

### 6.5 Catalog Extension

Catalog extension integrates new catalog, metadata, and discovery capabilities.

Extension entry points:

- `CatalogService`;
- `CatalogFormatter`;
- Catalog-related `ActionMethod`.

Extensible contents:

- dataset discovery;
- dataframe discovery;
- schema query;
- statistics;
- documentation;
- RDF / JSON-LD metadata formats;
- version management after PUT;
- lineage and reproducibility information.

Development requirements:

- Catalog queries should not directly read large data.
- Schema and statistics may be computed on demand or precomputed.
- New metadata fields should be backward compatible.
- External catalog unavailability should return `SYS_BACKEND_UNAVAILABLE`.
- Catalog results should avoid exposing internal storage paths.

### 6.6 Cook Extension

Cook extension adds algorithm execution, Flow scheduling, and operator repository integration.

Extension directions:

- new `TransformOp`;
- new Flow node;
- new executor;
- Repository Operator integration;
- Job status extension;
- resource quotas, retry, cache, and isolation strategies.

Recipe / Flow development requirements:

- Input parameters must be validatable.
- Output schema should be declared where possible.
- Jobs must have `jobId`.
- Job status must be queryable.
- Execution results must be registered as tickets.
- Job failures must keep diagnosable errors.
- Raw large data should not be returned by default.

Executor development requirements:

- Execution environment should be isolated from the server main process.
- Working directory should be isolated by job.
- Resource quotas should be configurable.
- Timeout should be configurable.
- Dependency cache should be isolated by version or hash.
- User code must not hold data access capability beyond its authorization scope.

Operator repository integration requirements:

- Support fixed versions.
- Production should not default to latest.
- Parameters must be validated by schema.
- Artifact pull failures should fail fast.
- Local cache should have TTL and capacity limits.
- Job records should keep operatorId / operatorVersion.

### 6.7 Proxy Extension

Proxy extension provides protocol-level forwarding and backend topology hiding.

Extension directions:

- transparent forwarding;
- resource-based routing;
- user-based routing;
- load balancing;
- Action re-wrapping;
- ticket re-registration;
- backend connection pooling;
- timeout, retry, and circuit breaking.

Development requirements:

- External clients must not see backend addresses.
- External clients must not see backend tickets.
- Result-producing Actions must re-register proxy-side tickets.
- traceId is a planned capability. Once implemented, downstream errors should keep the original traceId.
- traceId is a planned capability. Once implemented, proxy-side responses should add proxyTraceId.
- Backend credentials and client credentials should be isolated.
- Proxy logs should be able to correlate client requests and backend requests.

## 7. Complete Example: Developing a New Module

This section uses `AccessLogModule` as an example.

### 7.1 Goal

`AccessLogModule` should:

- record Action requests;
- record GetStream / PutStream start and end events;
- record user, request type, resource identifier, duration, row count, byte count, and error code;
- support a configuration switch;
- avoid blocking the main request path.

### 7.2 Create a Maven Submodule

Recommended module name:

```text
access-log-module
```

Recommended dependencies:

- depend on `dftp-common` for common models;
- depend on `dftp-server` for module interfaces and server context;
- do not depend on private implementations of `catalog-module`, `cook-module`, or `dacp-proxy`.

### 7.3 Implement the Module Class

Example:

```scala
public class AccessLogModule implements DftpModule {
    @Override
    public void init(ModuleAnchor anchor, ServerContext serverContext) {
        AccessLogConfig config = AccessLogConfig.from(serverContext);
        AccessLogWriter writer = new AsyncAccessLogWriter(config);

        anchor.registerEventHandler(new AccessLogEventHandler(writer));
    }
}
```

Implementation focus:

- read whether the module is enabled;
- initialize asynchronous log writer;
- register event handler;
- release writer when the service shuts down.

### 7.4 Implement Handler

The handler should process:

- Action request start;
- Action request end;
- Stream request start;
- Stream request end;
- request exception;
- job status change.

Planned log fields:

```json
{
  "traceId": "...",
  "user": "...",
  "actionType": "...",
  "resource": "...",
  "startTime": "...",
  "endTime": "...",
  "durationMs": 10,
  "rows": 1000,
  "bytes": 4096,
  "status": "SUCCESS",
  "errorCode": null
}
```

### 7.5 Add Configuration

Example:

```text
accesslog.enabled=true
accesslog.async=true
accesslog.queueSize=10000
accesslog.output=file
accesslog.file.path=logs/access.log
```

Requirements:

- `queueSize` must have an upper bound.
- Log write failures must not affect the data plane.
- Remaining logs should be flushed when the service shuts down.

### 7.6 Assemble the Module

Example:

```xml
<bean class="org.example.dacp.accesslog.AccessLogModule"/>
```

`AccessLogModule` should be placed later in the XML `modules` list so it can observe already registered business capabilities.

### 7.7 Write Tests

Test items:

- handler is registered when the module is enabled;
- handler is not registered when the module is disabled;
- Action requests generate access logs;
- Stream interruption generates exception logs;
- sensitive fields are masked;
- a full asynchronous queue does not block the main path.

## 8. Client Development Guide

### 8.1 DftpClient Base Capabilities

`DftpClient` provides base DFTP capabilities:

- establish Flight connections;
- authenticate;
- execute Action;
- execute GetStream by ticket;
- execute PutStream by ticket;
- wrap remote DataFrame as an iterable object.

Client requirements:

- Connections should support timeout configuration.
- Requests should carry authentication information.
- Errors should preserve server-side error codes.
- Streams must be closed after use.
- Clients must not assume server local path structure.

### 8.2 DacpClient High-Level API

`DacpClient` provides DACP-level APIs:

- Catalog query;
- schema query;
- Cook Recipe submission;
- Cook Flow submission;
- Job status query;
- Job result reading.

Wrapping principles:

- High-level APIs hide actionType details.
- Necessary low-level error information should be preserved.
- Results should support lazy reading.
- Large results should not be collected into memory by default.

### 8.3 RemoteDataFrameProxy Usage

Remote DataFrame should be consumed in streaming mode.

Requirements:

- support iterative reading;
- support explicit close;
- support read interruption;
- support schema pre-read;
- do not cache full data by default.

### 8.4 Client Error Handling

Clients need to handle:

- authentication failure;
- unsupported actionType;
- invalid parameters;
- missing or expired ticket;
- server-side Stream interruption;
- backend unavailability;
- Cook job failure;
- Proxy downstream error.

Error handling requirements:

- Preserve `code`.
- Preserve `traceId` if the server returns it.
- Preserve `proxyTraceId` in Proxy scenarios if it is returned.
- Do not wrap all errors as generic RuntimeException.
- Distinguish retryable errors from non-retryable errors.

## 9. Configuration Development Guide

### 9.1 Configuration File Structure

Baseline configuration is divided into two categories:

- property configuration: `dftp.conf` / `dacp.conf`;
- module assembly configuration: `dftp.xml` / `dacp.xml`.

Property configuration manages runtime parameters. Module assembly configuration declares which modules are loaded.

### 9.2 Configuration Naming Rules

Configuration keys should be grouped by module or capability prefix:

```text
dftp.host.position
dftp.host.port
dftp.scheme
ticket.ttlSeconds
ticket.maxEntries
catalog.rootDir
cook.maxConcurrentJobs
cook.jobTimeoutMs
proxy.backend.url
proxy.connectTimeoutMs
accesslog.enabled
```

Naming requirements:

- Use lowercase and dot separators.
- Prefix must identify the module.
- Include time units in key names, for example `timeoutMs`.
- Use `enabled` for boolean switches.
- Different modules must not reuse the same key with different meanings.

### 9.3 Required Configuration and Defaults

Each module must declare:

- required configuration;
- optional configuration;
- default values;
- valid ranges;
- invalid value handling;
- whether hot update is supported.

The baseline version should not support module hot plug/unplug. If partial hot configuration update is supported, it must be documented per key.

### 9.4 Sensitive Configuration Handling

Sensitive configuration includes:

- passwords;
- tokens;
- private keys;
- external service credentials;
- database credentials;
- object storage keys.

Requirements:

- Logs must mask sensitive values.
- Error responses must not carry plaintext secrets.
- Configuration dumps must not print full values.
- Test configuration must not commit real keys.

## 10. Error Codes and Exception Handling

### 10.1 Unified Error Model

Action error responses are recommended to use the following structure. The current implementation does not yet provide unified `traceId`; that field is planned:

```json
{
  "code": "REQ_INVALID_ARGUMENT",
  "message": "invalid request argument",
  "traceId": "...",
  "details": {}
}
```

Field descriptions:

- `code`: stable error code for programmatic handling.
- `message`: human-readable error description.
- `traceId`: chain tracing ID, planned field.
- `details`: machine-readable additional fields.

### 10.2 Error Code Segments

Recommended error code segments:

- `AUTH_*`: authentication failure, missing credentials, malformed credentials;
- `PERM_*`: permission denied, unauthorized operation;
- `REQ_*`: invalid request parameter, schema mismatch, unsupported operation;
- `DATA_*`: data source unreachable, file missing, unsupported format, ticket missing;
- `JOB_*`: job missing, job failed, job timeout, state conflict;
- `SYS_*`: internal system error, dependency unavailable.

### 10.3 Flight Status Mapping

Recommended mapping:

| Business Code | HTTP Semantics | Flight Status | Scenario |
|---|---:|---|---|
| `AUTH_INVALID_CREDENTIALS` | 401 | `UNAUTHENTICATED` | Invalid credentials |
| `AUTH_MISSING_CREDENTIALS` | 401 | `UNAUTHENTICATED` | Missing credentials |
| `PERM_ACCESS_DENIED` | 403 | `UNAUTHORIZED` | No permission |
| `DATA_NOT_FOUND` | 404 | `NOT_FOUND` | Data or ticket missing |
| `JOB_NOT_FOUND` | 404 | `NOT_FOUND` | Job missing |
| `REQ_INVALID_ARGUMENT` | 400 | `INVALID_ARGUMENT` | Invalid parameter |
| `REQ_UNSUPPORTED_OPERATION` | 400 | `INVALID_ARGUMENT` | Unsupported operation |
| `DATA_ALREADY_EXISTS` | 409 | `ALREADY_EXISTS` | PUT target conflict |
| `JOB_CONFLICT` | 409 | `FAILED_PRECONDITION` | Job state conflict |
| `JOB_TIMEOUT` | 408 | `TIMED_OUT` | Job timeout |
| `SYS_BACKEND_UNAVAILABLE` | 503 | `UNAVAILABLE` | Backend unavailable |
| `SYS_INTERNAL_ERROR` | 500 | `INTERNAL` | Unclassified internal error |

### 10.4 Module Exception Handling Requirements

Module exception handling requirements:

- Parameter errors should fail fast at Action entry.
- Permission errors must fail before data access.
- External dependency errors must not be wrapped as parameter errors.
- Server logs should record full exceptions.
- Client responses must not expose sensitive details.
- Unclassified errors should map to `SYS_INTERNAL_ERROR`.

### 10.5 Proxy Error Wrapping Rules

When Proxy forwards errors:

- preserve downstream `code`;
- once traceId is implemented, preserve downstream `traceId`;
- once traceId is implemented, add proxy-side `proxyTraceId`;
- do not expose sensitive internal topology when marking backend address;
- return `SYS_BACKEND_UNAVAILABLE` when backend is unreachable.

## 11. Testing Guide

### 11.1 Unit Tests

Unit tests should cover:

- common model serialization;
- TransformOp codec;
- parameter validation;
- error code mapping;
- module configuration parsing;
- pure handler logic.

### 11.2 Integration Tests

Integration tests should cover:

- module assembly;
- `KernelModule` routing;
- authentication chain;
- Action calls;
- ticket registration and lookup;
- GetStream / PutStream;
- Catalog query;
- Cook job status transition;
- Proxy ticket re-registration.

### 11.3 End-to-End Tests

Recommended end-to-end scenario:

1. Start DACP Server.
2. Authenticate client.
3. Query datasets through Catalog.
4. GET DataFrame.
5. Submit Cook Recipe.
6. Query Job status.
7. Read Cook result.
8. Repeat GET or Cook result reading through Proxy.

### 11.4 Module Extension Test Template

Each new module should verify:

- it can be loaded;
- it can be disabled if it implements an enabled switch;
- missing configuration fails;
- invalid configuration fails;
- handler is registered correctly;
- handler conflict can be detected;
- request can be routed;
- exceptional paths return standardized errors;
- resources are released.

### 11.5 Performance Tests

Performance tests should focus on:

- RecordBatch size;
- single-stream throughput;
- concurrent stream count;
- ticket count;
- ticket cleanup cost;
- concurrent Cook jobs;
- Proxy forwarding overhead;
- Catalog large-directory query latency.

Performance test output should include:

- test data size;
- concurrency;
- average latency;
- P95 / P99;
- throughput;
- CPU / memory;
- error count.

## 12. Packaging and Deployment

### 12.1 packaging Module

`packaging` generates deployable distributions.

Responsibilities:

- aggregate server jars;
- aggregate module dependencies;
- provide startup scripts;
- provide default configuration;
- provide example XML;
- output DFTP / DACP / Proxy service packages.

### 12.2 Distribution Layout

Recommended distribution layout:

```text
bin/
  dftp.sh
  dacp.sh
  dacp-proxy.sh
conf/
  dftp.conf
  dacp.conf
  dftp.xml
  dacp.xml
lib/
  *.jar
logs/
  ...
```

### 12.3 Startup Scripts

Startup scripts should support:

- specifying configuration directory;
- specifying log directory;
- specifying JVM options;
- foreground start;
- background start;
- stopping service;
- checking service status.

### 12.4 Module Enable/Disable

Currently, module enable/disable is controlled by XML assembly. Add a bean to the `modules` list to enable a module. Remove or comment out the bean to disable it.

Requirements:

- Unassembled modules must not register handlers.
- Missing configuration for unassembled modules should not affect startup.
- If a module implements its own `enabled` configuration, it must not register handlers when disabled.
- If a dependency module is disabled, the dependent module should fail startup and explain the reason.

### 12.5 Production Deployment Recommendations

Production recommendations:

- use fixed ports and service names;
- disable anonymous authentication;
- set ticket TTL and capacity limits;
- set Cook concurrency limits;
- set Proxy timeout and retry;
- enable access logs;
- send logs to a centralized search system;
- use fixed operator versions;
- define cleanup strategy for data directories and temporary directories.

## 13. Observability and Troubleshooting

### 13.1 Logging Rules

Current log fields follow the existing implementation. Planned structured logs should include at least:

- timestamp;
- level;
- traceId;
- module;
- actionType;
- user;
- resource;
- durationMs;
- errorCode;
- message.

### 13.2 traceId Propagation (Planned)

The current implementation does not provide a unified traceId propagation mechanism. Planned requirements:

- client may pass traceId;
- server generates one when missing;
- Action response returns traceId;
- Stream logs carry traceId;
- Cook Job records carry traceId;
- Proxy preserves downstream traceId and adds proxyTraceId.

### 13.3 AccessLogModule Recommendation

Access logs should be implemented as an independent module, without intruding into business modules.

Recommended recording points:

- Action request entry;
- Action request end;
- GetStream start;
- GetStream end;
- PutStream start;
- PutStream end;
- Job submission;
- Job status change;
- Job result reading.

### 13.4 Common Issues

Module not loaded:

- Check whether class in XML is correct.
- Check whether jar is in `lib/`.
- Check whether module bean is in the `modules` list.
- If the module implements its own `enabled` configuration, check whether it is `true`.
- Check module loading order in startup logs.

Action not routed:

- Check whether actionType is consistent.
- Check whether ActionMethod is registered.
- Check whether actionType conflicts exist.
- Check whether `KernelModule` has collected handlers.

Ticket missing:

- Check whether Action registered the ticket successfully.
- Check whether the ticket expired.
- Check whether Proxy completed ticket re-registration.
- Check whether the client connected to the wrong service node.

Stream interrupted:

- Check whether the client closed early.
- Check whether data source reader failed.
- Check network connection.
- Check RecordBatch size.
- Check server error logs.

Cook job failed:

- Check Recipe / Flow parameters.
- Check input data permission.
- Check executor working directory.
- Check resource quotas.
- Check operator version and dependencies.
- Check Job error details.

Proxy request failed:

- Check backend address.
- Check backend authentication.
- Check connection pool and timeout.
- If traceId is implemented, check downstream traceId.
- Check proxy-side ticket re-registration.

## 14. Development Conventions

### 14.1 Code Style

Requirements:

- clear naming;
- single-responsibility methods;
- no dependency on private implementations across modules;
- stable common models;
- explicit exception handling;
- tests covering core paths.

### 14.2 Naming Conventions

Recommendations:

- module classes end with `Module`;
- Action handlers end with `ActionMethod` or `ActionHandler`;
- Stream handlers end with `GetStreamMethod` / `PutStreamMethod`;
- configuration classes end with `Config`;
- service classes end with `Service`;
- client classes end with `Client`.

### 14.3 Action Naming

Action names should:

- use uppercase;
- use underscores;
- start with module prefix;
- express action semantics;
- remain stable once released.

Examples:

```text
CATALOG_LIST_DATASETS
CATALOG_GET_DATAFRAME_SCHEMA
COOK_SUBMIT_FLOW
COOK_GET_JOB_STATUS
```

### 14.4 Compatibility Rules

Compatibility requirements:

- Add new fields instead of changing existing ones.
- Do not change existing field meanings.
- Do not reuse existing error codes for new semantics.
- Deleting fields requires a deprecation period.
- Protocol behavior changes must be documented in release notes.
- Production operators should use fixed versions.

### 14.5 Security Rules

Security requirements:

- Sensitive information is not logged by default.
- Authentication and authorization are handled separately.
- Cook execution follows least privilege.
- User code should be isolated from the server main process.
- Proxy credentials should be isolated from client credentials.
- PUT target paths must be validated to avoid unauthorized writes.
- GET resource paths must be validated to avoid unauthorized reads.

## 15. Appendix

### 15.1 Core Interface List

Core interfaces:

- `DftpModule`
- `AuthenticationMethod`
- `ActionMethod`
- `GetStreamMethod`
- `PutStreamMethod`
- `CatalogService`
- `CatalogFormatter`
- `FlowScheduler`
- `FlowExecutionContext`
- `RepositoryClient`

### 15.2 Module Development Checklist

Before delivering a new module, confirm:

- module responsibility is defined;
- input/output contract is defined;
- `DftpModule` is implemented;
- extension points are registered through events;
- configuration items are added;
- XML assembly example is added;
- initialization failure is handled;
- resource release is handled;
- unit tests are added;
- assembly tests are added;
- exception-path tests are added;
- logs are added; if traceId is implemented, traceId propagation is verified;
- sensitive information is masked;
- development document is updated.

### 15.3 Action Development Checklist

Before delivering a new Action, confirm:

- actionType is unique;
- request parameters are validated;
- permission is checked;
- large results are returned by ticket;
- error codes comply with conventions;
- if traceId is implemented, response contains traceId;
- successful path tests are added;
- parameter error tests are added;
- permission error tests are added.

### 15.4 Stream Development Checklist

Before delivering a new Stream handler, confirm:

- ticket lookup logic is correct;
- data is streamed by batches;
- client interruption is handled;
- underlying resources are closed;
- size and batch parameters are configurable;
- error mapping is correct;
- if traceId is implemented, logs contain traceId;
- large-data scenario tests are added;
- interruption scenario tests are added.

### 15.5 Configuration Template

```text
<module>.enabled=true
<module>.timeoutMs=30000
<module>.maxConcurrency=16
<module>.cache.ttlSeconds=3600
<module>.log.enabled=true
```

### 15.6 Pre-Release Checklist

Before release, confirm:

- all tests pass;
- end-to-end scenarios pass;
- default configuration can start;
- module conflict checks pass;
- startup scripts are usable;
- documentation and configuration are consistent;
- error code table is updated;
- version changes are recorded.
