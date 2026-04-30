# DFTP-DACP 设计文档

[中文](./DFTP-DACP-Design-Document-ZH.md) | [English](DFTP-DACP-Design-Document.md)

## 目录

- [1. 文档概览](#1-文档概览)
  - [1.1 背景与动机](#11-背景与动机)
  - [1.2 设计目标](#12-设计目标)
- [2. 总体架构](#2-总体架构)
  - [2.1 系统分层](#21-系统分层)
  - [2.2 核心组件](#22-核心组件)
    - [2.2.1 DftpServer（服务容器）](#221-dftpserver服务容器)
    - [2.2.2 KernelModule（协议路由内核）](#222-kernelmodule协议路由内核)
    - [2.2.3 Client Gateway（客户端入口）](#223-client-gateway客户端入口)
    - [2.2.4 TicketManager（票据生命周期管理）](#224-ticketmanager票据生命周期管理)
    - [2.2.5 Action Handler Family（控制面处理器族）](#225-action-handler-family控制面处理器族)
    - [2.2.6 Stream Handler Family（数据面处理器族）](#226-stream-handler-family数据面处理器族)
    - [2.2.7 DacpCatalogModule（目录与元数据）](#227-dacpcatalogmodule目录与元数据)
    - [2.2.8 DacpCookModule（作业执行）](#228-dacpcookmodule作业执行)
    - [2.2.9 DacpServerProxy（代理转发）](#229-dacpserverproxy代理转发)
    - [2.2.10 Packaging/ServerStart（装配与部署入口）](#2210-packagingserverstart装配与部署入口)
- [3. 协议设计](#3-协议设计)
  - [3.1 核心概念](#31-核心概念)
  - [3.2 统一交互模型（控制面 + 数据面）](#32-统一交互模型控制面--数据面)
  - [3.3 认证（Authentication）](#33-认证authentication)
  - [3.4 GET：正向供给（取数据）](#34-get正向供给取数据)
  - [3.5 PUT：正向供给的反向操作（送数据/版本）](#35-put正向供给的反向操作送数据版本)
  - [3.6 COOK：反向供给（送算法到数据端）](#36-cook反向供给送算法到数据端)
    - [3.6.1 Recipe（概念模型）](#361-recipe概念模型)
    - [3.6.2 COOK 的输出](#362-cook-的输出)
    - [3.6.3 与算子仓库的结合点](#363-与算子仓库的结合点)
    - [3.6.4 与当前实现的对应关系](#364-与当前实现的对应关系)
- [4. 扩展设计](#4-扩展设计)
  - [4.1 Catalog 扩展（目录/元数据）](#41-catalog-扩展目录元数据)
  - [4.2 Cook 扩展（执行器/算子）](#42-cook-扩展执行器算子)
  - [4.3 Proxy 扩展（协议级代理）](#43-proxy-扩展协议级代理)
- [5. 数据模型设计](#5-数据模型设计)
  - [5.1 DataFrame / Row / StructType](#51-dataframe--row--structtype)
  - [5.2 Blob](#52-blob)
  - [5.3 TransformOp / Flow / Job](#53-transformop--flow--job)
- [6. 安全设计](#6-安全设计)
  - [6.1 认证与身份](#61-认证与身份)
  - [6.2 授权与最小权限（面向 COOK）](#62-授权与最小权限面向-cook)
  - [6.3 隔离与沙箱（规划）](#63-隔离与沙箱规划)
  - [6.4 审计与可观测](#64-审计与可观测)
- [7. 可扩展性设计](#7-可扩展性设计)
  - [7.1 模块化扩展机制（系统核心）](#71-模块化扩展机制系统核心)
  - [7.2 扩展点清单](#72-扩展点清单)
  - [7.3 扩展生命周期与装配流程](#73-扩展生命周期与装配流程)
  - [7.4 扩展优先级与冲突处理](#74-扩展优先级与冲突处理)
  - [7.5 稳定性边界与兼容性约束](#75-稳定性边界与兼容性约束)
  - [7.6 扩展实现模板（研发约定）](#76-扩展实现模板研发约定)
- [8. 工程实现视图（研发）](#8-工程实现视图研发)
  - [8.1 模块依赖关系（Maven）](#81-模块依赖关系maven)
  - [8.2 关键时序（研发实现约束）](#82-关键时序研发实现约束)
  - [8.3 配置矩阵（首版）](#83-配置矩阵首版)
  - [8.4 异常与错误语义（建议落地）](#84-异常与错误语义建议落地)
  - [8.5 测试策略（与模块对齐）](#85-测试策略与模块对齐)
- [9. 总结](#9-总结)

## 1. 文档概览

### 1.1 背景与动机

在科学计算与分布式数据协作场景中，“取数/用数”的痛点高度集中在两件事上：

- **数据越来越大**：从 GB 迅速增长到 TB~PB，数据“搬家”会带来高昂的网络、存储与等待成本。
- **数据越来越难用**：半结构化/结构化数据混合存在；传统 FTP/HTTP 往往需要脚本式裁剪、断点续传与格式适配，开发体验与吞吐效率都不理想。

因此需要一套面向“科学数据”的协议栈，使数据访问具备：

- **像访问网页一样简单**：统一的 URL 表达资源，统一的请求与返回语义；
- **像本地文件一样快**：对列裁剪、并行读取、压缩友好；
- **像直播一样边下边用**：以流式二维数据帧连续推送，第一帧到达即可开算。

本项目采用“协议栈”设计：

- **DFTP（Data Frame Transfer Protocol）**：提供底层传输与控制面能力（认证、GET/PUT/Action、ticket 化流式读取），并以 Arrow Flight 作为高性能 RPC/数据承载。
- **DACP（Data Access & Collaboration Protocol）**：在 DFTP 的运行时与模块机制之上，扩展出目录/元数据（Catalog）、反向供给执行（Cook/COOK）、权限与代理等上层能力。

> 供给视角：**GET 是正向供给（把数据端的“菜”端回家）**；**COOK 是反向供给（把“厨师/算法”送上门，数据原地做饭）**。

### 1.2 设计目标

- **统一数据抽象**：以 `DataFrame` 表达结构化流数据，以 `Blob` 表达非结构化二进制流；两者共享一套传输与访问机制。
- **二维流帧（SDF）能力**：用“多行多列、带类型”的二维帧作为网络传输单位，支持按行/按帧流式消费。
- **高性能传输**：基于 Apache Arrow Flight，减少序列化开销，支持高吞吐、低延迟的数据流交换。
- **列裁剪与高效读取**：底层偏列式读取能力，支持“只取需要的列”，降低 IO 与网络成本。
- **断点续传与复取**：帧级续传、可重放读取；资源以统一 URL/标识复取。
- **控制面/数据面解耦**：控制面返回 `ticket + 元信息`，数据面通过 `ticket` 拉取/推送流，便于惰性读取、代理转接与资源管理。
- **反向供给（COOK）**：把算法/配方送到数据端执行，优先返回结果/特征/聚合，显著降低数据搬迁。
- **安全与隔离**：认证、最小权限、一致的审计与可选的沙箱隔离机制。
- **可扩展性**：通过模块化扩展点（认证、GET/PUT、Action、目录服务、执行器等）支持按需装配和二次开发。

## 2. 总体架构

### 2.1 系统分层

系统按职责划分为四层：

1. **数据与算子抽象层（`dftp-common`）**
    - 数据结构：`DataFrame`、`Row`、`StructType`、`Blob`、`DataFrameMetaData`
    - 执行表达：`TransformOp`（Source/Map/Filter/Limit/Select 等）及其 JSON 表达
    - 通用能力：编解码、数据转换、认证模型等

2. **DFTP 协议内核层（`dftp-server` / `dftp-client`）**
    - 服务端：`DftpServer` 作为统一容器，承载 Arrow Flight Server
    - 客户端：`DftpClient` 提供认证、Action 调用、GET/PUT 流访问
    - 关键机制：`ticket` 注册与流式读取（控制面/数据面分离）

3. **DACP 扩展能力层（`catalog-module` / `cook-module` / `dacp-proxy`）**
    - Catalog：数据集/数据帧发现、RDF/JSON-LD 元数据、Schema/文档/统计等
    - Cook：COOK/作业提交、flow/recipe 执行、状态/进度/结果查询
    - Proxy：协议级代理转发与 ticket 重注册

4. **客户端与发行层（`dacp-client` / `packaging`）**
    - `DacpClient`：封装 Catalog/Cook 的高层 API
    - `packaging`：生成 DFTP/DACP/Proxy 的可部署发行包与配置模板

### 2.2 核心组件

#### 2.2.1 DftpServer（服务容器）

职责：

- 启动/停止 Arrow Flight Server；
- 提供 `ServerContext`（host/port/scheme、ticket 注册、dftpHome、密钥信息）；
- 装载并初始化模块体系。

关键接口：`DftpServer.start(...)`  
主要依赖：`dftp-server`、`KernelModule`、各 `DftpModule`

#### 2.2.2 KernelModule（协议路由内核）

职责：

- 聚合并路由四类协议扩展点；
- 在控制面和数据面之间做统一分发；
- 维持“内核稳定、能力可插拔”的边界。

关键接口：`AuthenticationMethod`、`ActionMethod`、`GetStreamMethod`、`PutStreamMethod`  
主要依赖：`dftp-server`、`dftp-common`

#### 2.2.3 Client Gateway（客户端入口）

职责：

- 对上提供统一调用入口（认证、Action、流读取/写入）；
- 对下封装 Flight 连接和远程数据代理；
- 向 DACP 上层 API 屏蔽底层调用细节。

关键接口：`DftpClient`、`DacpClient`、`RemoteDataFrameProxy`  
主要依赖：`dftp-client`、`dacp-client`、`dftp-common`

#### 2.2.4 TicketManager（票据生命周期管理）

职责：

- 管理 `ticket -> DataFrameHandle/BlobHandle` 注册关系；
- 处理过期、清理和失效回收；
- 为 Proxy 重注册提供统一票据抽象。

关键接口：通过 `ServerContext` 的 ticket 注册/查询能力暴露  
主要依赖：`DftpServer`、`KernelModule`、`dftp-common`

#### 2.2.5 Action Handler Family（控制面处理器族）

职责：

- 处理 GET/PUT 参数协商、Catalog/Cook/Proxy 等控制面请求；
- 统一返回 JSON 或 `ticket + metadata`；
- 承担参数校验、权限检查和错误语义收敛。

关键接口：`ActionMethod` 及各模块 Action 枚举  
主要依赖：`KernelModule`、`catalog-module`、`cook-module`、`dacp-proxy`

#### 2.2.6 Stream Handler Family（数据面处理器族）

职责：

- 执行真正的数据流读写；
- 通过 ticket 拉取/推送 Arrow RecordBatch；
- 屏蔽 DataFrame 与 Blob 的底层传输细节。

关键接口：`GetStreamMethod`、`PutStreamMethod`  
主要依赖：`KernelModule`、`dftp-common`、底层数据源模块

#### 2.2.7 DacpCatalogModule（目录与元数据）

职责：

- 提供数据集/数据帧发现、Schema、统计、文档等能力；
- 输出 JSON-LD/RDF 等元数据表达；
- 通过 SPI 解耦底层目录实现。

关键接口：`DacpCatalogModule`、`CatalogService`、`CatalogFormatter`  
主要依赖：`catalog-module`、`dftp-server`、`dftp-common`

#### 2.2.8 DacpCookModule（作业执行）

职责：

- 处理 `SUBMIT_RECIPE`、`SUBMIT_FLOW`、作业状态查询；
- 调度执行 TransformTree/Flow；
- 输出作业结果 ticket 供客户端流式读取。

关键接口：`DacpCookModule`、`FlowScheduler`、`FlowExecutionContext`  
主要依赖：`cook-module`、`dftp-server`、`dftp-common`

#### 2.2.9 DacpServerProxy（代理转发）

职责：

- 作为统一入口转发到后端 DACP 服务；
- 对结果型请求执行 ticket 重注册；
- 隐藏后端拓扑与内部地址。

关键接口：`DacpServerProxy`  
主要依赖：`dacp-proxy`、`dftp-server`、`dftp-client`

#### 2.2.10 Packaging/ServerStart（装配与部署入口）

职责：

- 提供 `dftp.sh`/`dacp.sh`/`dacp-proxy.sh` 启动脚本；
- 负责模块装配、配置加载与发行包生成；
- 统一测试、开发、生产环境的部署入口。

关键接口：`ServerStart`、`FileDirectoryDataSourceModule`、assembly 配置  
主要依赖：`packaging`、各运行模块

## 3. 协议设计

### 3.1 核心概念

- **二维流帧（Streaming DataFrame / SDF）**：网络传输单位是一张“带 schema 的二维小表”，可按帧推送、按行消费；第一帧到达即可开算。
- **DACP URL / 资源标识**：对数据资源（以及 PUT 后的新版本）提供可复取的统一标识，便于分享、复现与协作。
- **Ticket**：用于把控制面响应与数据面流传输解耦。控制面返回 ticket；数据面通过 ticket 拉取/推送 Arrow Flight 流。

### 3.2 统一交互模型（控制面 + 数据面）

系统的交互由三类能力组成：

- **Action（控制面）**：携带 JSON 参数，返回 JSON 或 `ticket + 元信息`（如 schema、size、blobType 等）。
- **GetStream（数据面读取）**：通过 `ticket` 拉取 Arrow Flight 数据流（DataFrame 或 Blob 承载流）。
- **PutStream（数据面写入）**：通过 `ticket` 推送 Arrow Flight 数据流（DataFrame 或 Blob 承载流）。

优点：

- 控制面轻量、易路由；
- 数据面可惰性消费、可中途停止；
- 代理节点可“重注册”ticket，屏蔽后端拓扑与 ticket 生命周期差异。

### 3.3 认证（Authentication）

目标：建立统一的身份模型，为权限控制、审计与代理透传提供依据。

基本流程：

1. 客户端使用 `credentials` 进行登录认证；
2. 服务端通过 `AuthenticationMethod` 链选择匹配的认证方式并产出 `UserPrincipal`；
3. 会话 token 与 `UserPrincipal` 映射，用于后续请求注入身份；
4. 后续 GET/PUT/Action 都可携带并使用该身份信息进行鉴权与审计。

### 3.4 GET：正向供给（取数据）

语义：客户端声明“我要什么”，服务端把数据按指定变换打包为二维流帧并流式推送。

关键能力：

- **列裁剪**：只取需要的列，避免多余 IO/网络开销；
- **边下边用**：二维帧到达即可开算；
- **帧级续传/复取**：以帧为切片单位更易恢复与重放；
- **统一资源标识**：数据通过 URL/标识定位与复取。

典型实现路径（落到当前系统）：

1. 客户端构造 `TransformOp`（可表达 source + select/filter/limit 等）；
2. 客户端发起控制面 Action（GET）；
3. 服务端执行变换树得到结果 DataFrame；
4. 服务端注册为 `ticket`，返回 `dataframeMetaData + ticket`；
5. 客户端基于 ticket 拉取 Arrow Flight 数据流并惰性消费。

### 3.5 PUT：正向供给的反向操作（送数据/版本）

语义：把本地生成的数据帧或中间结果送入数据端存储/管理，并获得可复取的统一标识（URL/版本）。

关键诉求：

- 结果可复取（“取餐码”式标识）；
- 适配版本化管理（设计层要求；具体策略可由 catalog/目录服务实现）。

典型实现路径（落到当前系统）：

1. 控制面 Action 获取上传 ticket（参数协商/预检）；
2. PutStream 推送 Arrow RecordBatch 流；
3. 服务端 PutStreamMethod 消费并写入目标数据源/目录。

### 3.6 COOK：反向供给（送算法到数据端）

语义：不搬运原始数据，而是把算法/配方交给数据端执行，仅把结果流回客户端。

#### 3.6.1 Recipe（概念模型）

Recipe 包含：

- `code`：内联代码（Python/R/Julia/SQL）或可序列化算子描述（与 `operatorRef` 二选一）
- `operatorRef`：仓库算子引用（`operatorId`、`operatorVersion`、`entrypoint`、`artifactType`）
- `operatorArgs`：仓库算子入参（需满足算子定义中的参数 schema）
- `env`：Conda/Docker/WASM 等运行环境信息
- `resource`：CPU/内存/GPU 配额与执行约束
- `outputSchema`：结果 schema 预声明（便于下游流式消费/校验）
- `params`：运行参数
- `token`：一次性/受限访问令牌（最小权限访问指定数据集/数据帧）

调用约束：

- 内联模式：提供 `code`，不提供 `operatorRef`
- 仓库模式：提供 `operatorRef`（可选 `operatorArgs`），`code` 可为空
- 若两者同时提供，默认以 `operatorRef` 为准（建议在服务端做显式校验并告警）

#### 3.6.2 COOK 的输出

COOK 的输出不应是“原始数据”，而应优先是：

- 结果表（二维流帧）
- 聚合/统计
- 特征抽取结果

#### 3.6.3 与算子仓库的结合点

COOK 与算子仓库（Repository Operator）的结合点建议定义为“控制面解析 + 执行面拉取 + 结果面回传”三段：

1. 控制面（提交阶段）：
   - `SUBMIT_RECIPE` / `SUBMIT_FLOW` 请求中允许直接声明仓库算子引用，如：`operatorId`、`operatorVersion`、`entrypoint`、`params`。
   - 服务端在入队前完成算子元数据校验（是否存在、版本是否可用、参数是否满足 schema 约束）。

2. 执行面（调度阶段）：
   - 调度器根据算子引用通过 `RepositoryClient` 拉取算子描述与运行材料（脚本、镜像、wheel/jar 等）。
   - `FlowExecutionContext` 负责把“算子定义 + 运行环境 + 数据访问 token”注入执行节点。
   - 若为多节点 Flow，节点间仅传递算子引用与中间结果句柄，不复制仓库大文件。

3. 结果面（回传阶段）：
   - 执行结果统一落到 `DataFrame`/`Blob`，注册 ticket 后由客户端拉流读取。
   - 作业记录保留 `operatorId/operatorVersion`，用于结果追溯与复现。

实现契约建议：

- 版本约束：支持固定版本、版本范围与“latest”策略（生产环境建议固定版本）。
- 参数契约：算子输入输出 schema 必须可校验，校验失败在提交阶段快速失败。
- 缓存策略：执行节点本地缓存仓库制品，按版本哈希复用并设置 TTL。
- 安全边界：只允许白名单仓库源，算子拉取与执行凭据分离，避免越权访问。

#### 3.6.4 与当前实现的对应关系

当前工程中 COOK 类能力由 `cook-module` 对外提供 Action：

- `SUBMIT_RECIPE`：执行单个变换树并返回结果 DataFrame 的 ticket
- `SUBMIT_FLOW`：提交 flow 并返回 `jobId`
- `GET_JOB_STATUS`：查询作业状态
- `GET_JOB_EXECUTE_PROCESS`：查询进度/吞吐
- `GET_JOB_EXECUTE_RESULT`：查询结果并返回（结果 DataFrame ticket 列表/映射）

总结：**COOK 复用 DFTP 的 ticket + stream 作为结果回传通道**，实现“算法动、数据不动、结果流回”。

## 4. 扩展设计

### 4.1 Catalog 扩展（目录/元数据）

扩展入口：

- SPI：`CatalogService`
- 模块：`catalog-module`

可扩展点：

- 数据集/数据帧发现来源：目录、对象存储、数据湖、数据库、外部编目系统
- RDF/JSON-LD 元数据词表与 schema 扩展（科研领域本体/领域词汇）
- 文档/统计信息生成策略（按需预计算、运行时计算、采样统计等）
- “复取地址/版本管理”策略（PUT 后如何生成 URL、如何追溯 lineage）

### 4.2 Cook 扩展（执行器/算子）

可扩展点：

- 变换表达扩展：新增 `TransformOp`/Flow 节点类型
- 外部算子仓库：Repository Operator、版本选择与参数约束
- 多语言执行：Python/R/Julia/SQL 执行器适配
- 运行环境：容器/Docker、WASM、Conda 环境、依赖缓存
- 作业治理：结果缓存与过期清理、持久化、失败重试、限流与资源配额

### 4.3 Proxy 扩展（协议级代理）

可扩展点：

- 转发策略：透明转发 vs 对关键动作（GET/COOK 结果）重封装
- ticket 重注册策略：如何统一由代理发放 ticket，屏蔽后端差异
- 内部连接治理：凭据隔离、缓存与连接池、超时与熔断

## 5. 数据模型设计

### 5.1 DataFrame / Row / StructType

- `StructType`：描述二维帧的 schema（字段名、类型、可空、顺序）
- `Row`：行数据
- `DataFrame`：流式表格抽象，可在客户端按需遍历/变换/收集

与 Arrow 的映射：

- schema 可映射为 Arrow Schema；
- 数据以 Arrow RecordBatch 批量传输；
- 帧/批大小可作为吞吐与延迟的调优参数。

### 5.2 Blob

- `Blob` 表示二进制流（非结构化）
- 设计上可用“单列 bytes 的 DataFrame 流”承载，从而复用 Put/GetStream 机制
- Blob 的元信息应包括大小与类型（MIME/自定义类型）

### 5.3 TransformOp / Flow / Job

建议抽象：

- **TransformOp**：表达“源 + 变换”的执行树（便于 GET 与 recipe 共享）
- **Flow**：表达 DAG/多输出流程（便于 COOK 作业化）
- **Job**：面向运维与可观测的作业实体：
    - `jobId`
    - `status`（RUNNING/FAILED/COMPLETE）
    - `progress`（可选）
    - `throughput`（rows/s）
    - `results`（输出名 -> ticket + schema）

## 6. 安全设计

### 6.1 认证与身份

- 认证通过 `AuthenticationMethod` 扩展点实现（匿名、用户名密码、密钥等可插拔）。
- 服务端把凭据映射为 `UserPrincipal`，用于权限判断、代理透传与审计记录。

### 6.2 授权与最小权限（面向 COOK）

依据协议之二，反向供给需要更严格的最小权限与隔离策略：

- **一次性 Token**：Recipe/COOK 请求中携带的一次性 token 仅可访问被授权的数据集/数据帧，用完即焚。
- **操作级权限**：按“数据对象 + 操作类型（读/写/算子/导出）”进行权限检查，而不仅仅是连接认证。
- **多租户隔离**：同一节点上不同用户的作业与数据访问互不影响（目录/执行环境隔离）。

### 6.3 隔离与沙箱（规划）

当前版本状态：**未实现**（目标在 M3/M4 逐步落地）。

为防止“算法上门”带来的风险，拟按以下方向实现：

- **沙箱容器**：只读文件系统 + 受限网络；
- **资源配额**：CPU/内存/GPU 限额与执行超时；
- **依赖控制**：镜像/环境白名单与依赖缓存复用策略；
- **数据访问边界**：只允许访问 token 授权的数据路径/数据集。

### 6.4 审计与可观测

计划新增一个独立模块：`AccessLogModule`，通过 `DftpModule` 挂载到服务端，统一记录访问日志（Access Log）。

模块目标：

- 对 GET/PUT/COOK/Catalog/Proxy 等关键请求生成结构化访问日志；
- 不侵入业务模块，实现“按模块装配、按配置开关”；
- 与现有日志体系兼容（log4j2），支持后续接入 ELK/ClickHouse 等检索系统。

记录时机（建议）：

- Action 请求：请求进入、响应返回（含耗时）
- GetStream/PutStream：流开始、流结束、异常中断
- Job 类请求：提交、状态变更、结果读取

Access Log 建议字段：

- who：用户/凭据
- what：请求类型、数据对象、变换/算子摘要
- when：开始/结束时间、持续时长
- where：节点/容器/执行器
- how much：行数、字节数、吞吐、错误原因

实现约束（首版）：

- 日志必须带 `traceId`，可串联 Action 与 Stream 两条链路
- 敏感信息脱敏（密码、token、密钥材料）
- 高并发场景下采用异步写日志，避免阻塞数据面

## 7. 可扩展性设计

### 7.1 模块化扩展机制（系统核心）

可扩展性是本系统的核心设计。系统通过“模块 + 事件收集 + 处理器路由”三段式结构实现可插拔能力：

- 模块实现 `DftpModule`，在 `init(anchor, serverContext)` 中注册：
    - `EventHandler`（收集扩展点、处理事件）
    - `EventSource`（触发收集事件）
- KernelModule 收集并路由四类处理器：
    - Authentication / Action / GetStream / PutStream

架构价值：

- **内核稳定、能力可插拔**：协议内核只负责路由，不绑定业务逻辑；
- **装配式部署**：同一容器可装配成 DFTP Server、DACP Server 或 Proxy Server；
- **便于二次开发**：新增能力只需要新增模块并挂接到收集事件。

职责边界：

- `DftpServer`：生命周期与容器上下文管理
- `KernelModule`：扩展点聚合与请求路由
- 业务模块（Catalog/Cook/Proxy/AccessLog）：仅实现具体能力，不修改内核语义

### 7.2 扩展点清单

按扩展接口分层，研发可按“最小可用单元”增量实现：

1. 认证层扩展
   - 新增认证方式：实现 `AuthenticationMethod`
   - 典型场景：用户名密码、token、keypair、外部 IdP 映射

2. 控制面扩展
   - 新增控制面动作：实现 `ActionMethod`
   - 典型场景：Catalog 查询、Cook 提交、作业治理、审计查询

3. 数据面扩展
   - 新增 GET 资源/数据源：实现 `GetStreamMethod` 或新增 DataFrameProvider
   - 新增 PUT 目的地：实现 `PutStreamMethod`
   - 典型场景：文件系统、对象存储、数据库、缓存层

4. 执行与目录扩展
   - 新增目录来源：实现 `CatalogService`
   - 新增执行节点：扩展 `TransformOp`/Flow 节点与执行上下文
   - 算子仓库接入：扩展 `RepositoryClient` 与版本/参数校验

5. 平台能力扩展
   - 新增代理策略：扩展 Proxy 对关键动作重封装与 ticket 重注册逻辑
   - 新增审计能力：实现 `AccessLogModule` 记录结构化 Access Log

### 7.3 扩展生命周期与装配流程

新增模块从开发到运行建议遵循以下生命周期：

1. 设计期：定义扩展接口、输入输出契约、错误语义
2. 开发期：实现 `DftpModule` 与对应处理器，完成单测
3. 装配期：在 `ServerStart` 或部署配置中启用模块
4. 运行期：由 `init(anchor, serverContext)` 完成注册并生效
5. 观测期：通过日志/指标验证扩展行为与性能

运行时装配约束：

- 模块初始化必须幂等；重复加载不应产生重复注册
- 模块初始化失败应快速失败并给出可定位错误
- 不允许模块在运行期无控制地动态改写其他模块状态

### 7.4 扩展优先级与冲突处理

当同类扩展同时存在时，需要定义确定性行为，避免“同请求多处理器竞争”：

- 认证链：按注册顺序或显式优先级匹配，首个 `accepts=true` 的处理器生效
- Action 路由：按 actionType 精确匹配；冲突时以高优先级模块为准并记录告警
- Stream 路由：按资源类型/协议前缀匹配；无法匹配则返回标准化错误码

冲突治理建议（规划）：

- 提供模块优先级配置（如 `module.priority.<name>`）
- 启动时执行冲突检测并输出冲突清单
- 对覆盖行为输出审计日志，便于后续排障

### 7.5 稳定性边界与兼容性约束

为确保扩展生态长期可维护，建议明确以下边界：

- 内核 API 稳定面：`DftpModule`、四类 Method 接口、`ServerContext` 核心能力
- 可演进面：具体 Action 名称、模块内部实现、策略参数
- 禁止面：跨模块直接访问私有状态、绕过 `KernelModule` 直接处理协议请求

兼容性要求：

- 新增字段遵循向后兼容（优先追加，不破坏旧字段语义）
- 错误码新增不复用既有含义
- 协议行为变化需在版本发布中显式声明（含迁移指引）

### 7.6 扩展实现模板（研发约定）

建议每个新增模块遵循统一交付模板：

1. 模块定义：模块名、责任边界、依赖模块
2. 接口契约：请求参数、返回结构、错误码
3. 生命周期：初始化、运行、关闭/清理逻辑
4. 配置项：键名、类型、默认值、是否热更新
5. 测试项：单测、集成测试、失败注入测试
6. 观测项：日志字段、指标、告警阈值
7. 安全项：鉴权、脱敏、资源配额与隔离策略


## 8. 工程实现视图（研发）

### 8.1 模块依赖关系（Maven）

实现层建议按以下依赖方向维护，避免循环依赖：

- `dftp-common`：纯模型/协议公共层，被所有模块依赖
- `dftp-server`：依赖 `dftp-common`，提供服务端内核与模块容器
- `dftp-client`：依赖 `dftp-common`，提供基础客户端能力
- `catalog-module`：依赖 `dftp-server + dftp-common`，提供 Catalog Action
- `cook-module`：依赖 `dftp-server + dftp-common`，提供 Cook Action/Job
- `dacp-client`：依赖 `dftp-client + dftp-common`，封装 DACP 高层接口
- `dacp-proxy`：依赖 `dftp-server + dftp-client + dftp-common`，实现转发与 ticket 重注册
- `packaging`：聚合上述模块，输出发行包与启动脚本

约束：

- 公共数据模型只能沉淀在 `dftp-common`
- 业务模块（catalog/cook/proxy）不得反向依赖 `packaging`
- `dacp-client` 不引入服务端模块，保持纯客户端发布边界

### 8.2 关键时序（研发实现约束）

1. GET（DataFrame）
   1) Client 组装 `TransformOp` 并发起 Action  
   2) Server 执行变换并注册 `ticket -> DataFrameHandle`  
   3) Action 返回 `DataFrameMetaData + ticket`  
   4) Client 使用 ticket 发起 `getStream` 并流式消费  

2. PUT（DataFrame/Blob）
   1) Client 发起 Action 获取上传 ticket  
   2) Client 通过 `putStream` 推送 RecordBatch/Blob 分片  
   3) Server `PutStreamMethod` 落盘/入库并返回结果标识  

3. COOK（Flow/Recipe）
   1) Client 提交 `SUBMIT_RECIPE` 或 `SUBMIT_FLOW`  
   2) Server 创建 `jobId` 并进入调度执行  
   3) Client 轮询 `GET_JOB_STATUS/GET_JOB_EXECUTE_PROCESS`  
   4) 完成后通过 `GET_JOB_EXECUTE_RESULT` 获取结果 ticket 并读取流  

实现注意：

- ticket 生命周期必须与 `ServerContext` 绑定，支持过期清理
- Action 层不直接回传大结果，统一回传 ticket
- Proxy 对结果型 Action 必须重注册 ticket，避免客户端直接暴露后端地址

### 8.3 配置矩阵（首版）

首版建议统一收敛在 `dftp.conf`/`dacp.conf`，按模块前缀分组：

- 网络：`*.host.position`、`*.host.port`、`*.scheme`
- 模块装配（XML）：通过 `dftp.xml`/`dacp.xml` 动态注册模块（module class 列表、启停开关、顺序/优先级）
- 鉴权：匿名开关、用户名密码源、密钥路径（如启用 keypair）
- ticket：TTL、最大缓存数量、清理周期
- 数据源：root directory、允许格式、单文件大小上限
- Cook：并发作业上限、任务超时、临时目录、容器开关
- Proxy：后端地址、连接池、超时、重试
- 观测：日志级别、审计开关、指标上报间隔

XML 模块动态注册建议最小字段：

- `module.class`：模块实现类（必填）
- `module.enabled`：是否启用（默认 `true`）
- `module.order`：加载顺序（默认 `0`，越小越先）
- `module.configRef`：关联配置前缀（可选）

加载约束：

- 启动时完成模块解析与注册；运行期不做热插拔（首版）
- class 不存在或初始化失败时启动失败并输出明确错误
- 同类处理器冲突按 `module.order` 和模块优先级规则决策

### 8.4 异常与错误语义（建议落地）

建议统一错误响应模型：`code` + `message` + `traceId` + `details`。

错误码分段：

- `AUTH_*`：认证失败/凭据缺失/凭据格式错误
- `PERM_*`：权限不足、数据操作未授权
- `REQ_*`：请求参数错误（schema 不匹配、TransformOp 非法）
- `DATA_*`：数据源不可达、文件不存在、格式不支持
- `JOB_*`：作业不存在、作业失败、作业超时
- `SYS_*`：系统内部错误、依赖不可用

错误状态映射（业务码 -> HTTP 语义 -> Arrow Flight 状态）：

| 业务错误码 | HTTP 语义 | Arrow Flight 状态（建议） | 典型场景 |
|---|---:|---|---|
| `AUTH_INVALID_CREDENTIALS` | 401 | `UNAUTHENTICATED` | 凭据错误/登录失败 |
| `AUTH_MISSING_CREDENTIALS` | 401 | `UNAUTHENTICATED` | 未携带凭据 |
| `PERM_ACCESS_DENIED` | 403 | `UNAUTHORIZED` | 已认证但无权限 |
| `DATA_NOT_FOUND` | 404 | `NOT_FOUND` | 数据集、资源路径或 ticket 不存在 |
| `JOB_NOT_FOUND` | 404 | `NOT_FOUND` | `jobId` 不存在 |
| `REQ_INVALID_ARGUMENT` | 400 | `INVALID_ARGUMENT` | 参数校验失败、schema 不匹配 |
| `REQ_UNSUPPORTED_OPERATION` | 400 | `INVALID_ARGUMENT` | 不支持的 action/算子参数 |
| `DATA_ALREADY_EXISTS` | 409 | `ALREADY_EXISTS` | PUT 目标版本冲突 |
| `JOB_CONFLICT` | 409 | `FAILED_PRECONDITION` | 作业状态不允许当前操作 |
| `JOB_TIMEOUT` | 408 | `TIMED_OUT` | 作业执行超时 |
| `SYS_BACKEND_UNAVAILABLE` | 503 | `UNAVAILABLE` | 后端依赖不可用 |
| `SYS_INTERNAL_ERROR` | 500 | `INTERNAL` | 未分类内部异常 |

映射规则（统一约束）：

- 对外文档可用 HTTP 语义描述（如 404=资源不存在）；Flight 传输层统一返回对应 `CallStatus`。
- `code` 保持稳定、可枚举；`message` 可读；`details` 放机器可解析字段（如 `resourceId`、`jobId`、`field`）。
- 未命中映射表的异常默认归类为 `SYS_INTERNAL_ERROR -> INTERNAL`。
- Proxy 透传下游错误时需保留原始 `traceId`，并附加代理侧 `proxyTraceId`。

最小要求：

- 服务端日志记录完整异常堆栈；响应只返回必要错误信息
- 所有 Action 响应都带 `traceId`，便于跨模块排障

### 8.5 测试策略（与模块对齐）

- `dftp-common`：模型编解码、TransformOp 序列化、类型系统单测
- `dftp-client` / `dftp-server`：GET/PUT/认证/断连恢复集成测试
- `catalog-module`：Catalog Action 合约测试与元数据格式测试
- `cook-module`：Flow 调度、Job 状态流转、执行器隔离测试
- `dacp-proxy`：转发正确性、ticket 重注册一致性测试
- `packaging`：启动脚本与默认配置冒烟测试

CI 建议门禁：

- 单测必须通过
- 至少一套端到端场景（GET + COOK + Proxy）通过

## 9. 总结

DFTP-DACP 通过“**高性能流式二维数据帧 + 控制面/数据面解耦 + 模块化可插拔扩展**”三条主线，覆盖了科学数据访问的两种核心供给方式：

- **GET（正向供给）**：列裁剪、帧级流式、可复取，让“取数像点外卖”；
- **COOK（反向供给）**：把算法送到数据端执行，让“大数据不搬家，结果流回家”。

在此基础上，Catalog、Cook 与 Proxy 等模块能力可按场景装配，安全与隔离措施可逐步增强，使系统既能快速落地，又具备持续演进空间。
