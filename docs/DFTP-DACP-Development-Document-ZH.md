# DFTP-DACP 开发文档

[中文](./DFTP-DACP-Development-Document-ZH.md) | [English](./DFTP-DACP-Development-Document.md)

## 目录

- [1. 文档概览](#1-文档概览)
  - [1.1 文档目标](#11-文档目标)
  - [1.2 适用读者](#12-适用读者)
- [2. 工程结构说明](#2-工程结构说明)
  - [2.1 Maven 模块总览](#21-maven-模块总览)
  - [2.2 模块依赖方向](#22-模块依赖方向)
  - [2.3 关键工程边界](#23-关键工程边界)
- [3. 本地开发环境](#3-本地开发环境)
  - [3.1 环境要求](#31-环境要求)
  - [3.2 编译与测试](#32-编译与测试)
  - [3.3 启动服务](#33-启动服务)
- [4. 核心运行机制](#4-核心运行机制)
  - [4.1 DftpServer 生命周期](#41-dftpserver-生命周期)
  - [4.2 KernelModule 路由机制](#42-kernelmodule-路由机制)
  - [4.3 控制面与数据面分离](#43-控制面与数据面分离)
  - [4.4 Ticket 生命周期](#44-ticket-生命周期)
- [5. Module 扩展开发指南（核心）](#5-module-扩展开发指南核心)
  - [5.1 Module 扩展模型](#51-module-扩展模型)
  - [5.2 DftpModule 接口约定](#52-dftpmodule-接口约定)
  - [5.3 Module 生命周期](#53-module-生命周期)
  - [5.4 Module 装配方式](#54-module-装配方式)
  - [5.5 Module 加载顺序与冲突处理](#55-module-加载顺序与冲突处理)
  - [5.6 Module 初始化约束](#56-module-初始化约束)
  - [5.7 Module 配置规范](#57-module-配置规范)
  - [5.8 Module 资源管理](#58-module-资源管理)
  - [5.9 Module 日志与观测](#59-module-日志与观测)
  - [5.10 Module 测试要求](#510-module-测试要求)
- [6. 扩展点开发指南](#6-扩展点开发指南)
  - [6.1 Authentication 扩展](#61-authentication-扩展)
  - [6.2 Action 扩展](#62-action-扩展)
  - [6.3 GetStream 扩展](#63-getstream-扩展)
  - [6.4 PutStream 扩展](#64-putstream-扩展)
  - [6.5 Catalog 扩展](#65-catalog-扩展)
  - [6.6 Cook 扩展](#66-cook-扩展)
  - [6.7 Proxy 扩展](#67-proxy-扩展)
- [7. 新模块开发完整示例](#7-新模块开发完整示例)
- [8. 客户端开发指南](#8-客户端开发指南)
- [9. 配置开发指南](#9-配置开发指南)
- [10. 错误码与异常处理](#10-错误码与异常处理)
- [11. 测试指南](#11-测试指南)
- [12. 打包与部署](#12-打包与部署)
- [13. 可观测与排障](#13-可观测与排障)
- [14. 开发规范](#14-开发规范)
- [15. 附录](#15-附录)

## 1. 文档概览

### 1.1 文档目标

本文档面向 DFTP-DACP 的研发实现与二次开发，重点说明：

- 如何理解当前工程结构与模块边界；
- 如何在本地编译、测试、启动 DFTP/DACP 服务；
- 如何开发新的 `DftpModule`；
- 如何扩展认证、控制面 Action、数据面 GetStream/PutStream；
- 如何扩展 Catalog、Cook、Proxy 等 DACP 上层能力；
- 如何完成模块装配、配置、测试、打包、部署与排障。

本文档的核心是 **Module 扩展开发**。DFTP-DACP 的研发模型不是通过修改协议内核来增加业务能力，而是通过模块注册扩展点，由 `KernelModule` 统一收集并路由处理器。

### 1.2 适用读者

本文档适用于：

- 协议内核开发者；
- 服务端模块开发者；
- 数据源接入开发者；
- Catalog / Cook / Proxy 扩展开发者；
- 客户端 SDK 开发者；
- 打包、部署与运维人员。

## 2. 工程结构说明

### 2.1 Maven 模块总览

当前工程建议按以下职责理解：

- `dftp-common`：公共模型与协议抽象层，包含 `DataFrame`、`Row`、`StructType`、`Blob`、`TransformOp`、认证模型、编解码与通用工具。
- `dftp-server`：服务端协议内核层，包含 `DftpServer`、`ServerContext`、`KernelModule`、模块接口与四类核心扩展点。
- `dftp-client`：基础客户端层，封装 Flight 连接、认证、Action 调用、GET/PUT 流访问。
- `catalog-module`：DACP Catalog 能力模块，提供数据集/数据帧发现、元数据查询、Schema、统计与文档能力。
- `cook-module`：DACP Cook 能力模块，提供 Recipe/Flow 提交、作业调度、状态查询、进度查询与结果读取。
- `dacp-client`：DACP 高层客户端 API，封装 Catalog 和 Cook 的业务调用。
- `dacp-proxy`：DACP 协议级代理，提供后端转发、结果型 Action 重封装与 ticket 重注册。
- `packaging`：发行包装配模块，提供启动脚本、配置模板与可部署包。

### 2.2 模块依赖方向

依赖方向必须保持清晰，避免循环依赖：

- `dftp-common` 不依赖其他业务模块；
- `dftp-server` 依赖 `dftp-common`；
- `dftp-client` 依赖 `dftp-common`；
- 业务模块依赖 `dftp-server + dftp-common`；
- `dacp-client` 依赖 `dftp-client + dftp-common`；
- `dacp-proxy` 依赖 `dftp-server + dftp-client + dftp-common`；
- `packaging` 负责聚合，不承载协议内核或业务逻辑。

约束：

- 公共数据模型只能沉淀在 `dftp-common`；
- 业务模块不得反向依赖 `packaging`；
- 客户端模块不得依赖服务端模块；
- 模块之间不得通过私有实现互相调用，应通过公开接口或协议请求交互。

### 2.3 关键工程边界

研发时需要维护以下边界：

- 协议内核边界：`DftpServer` 和 `KernelModule` 负责生命周期、上下文、处理器收集与路由，不承载具体业务。
- 业务模块边界：Catalog、Cook、Proxy、AccessLog 等模块只实现自身能力，通过扩展点接入。
- 公共模型边界：跨模块共享的数据结构、错误模型、协议 DTO 应放在公共层。
- 客户端边界：客户端只调用远端服务，不直接引用服务端实现。
- 部署边界：`packaging` 只负责装配配置、脚本和发行包。

## 3. 本地开发环境

### 3.1 环境要求

建议开发环境：

- JDK：与项目 `pom.xml` 声明保持一致；
- Maven：建议使用 3.8+；
- IDE：IntelliJ IDEA 或其他支持 Maven 多模块工程的 IDE；
- 本地系统：macOS / Linux / Windows 均可，生产环境建议 Linux；
- 网络依赖：首次构建需要访问 Maven 仓库下载依赖。

开发前应确认：

```bash
java -version
mvn -version
```

### 3.2 编译与测试

完整编译：

```bash
mvn clean package
```

运行单元测试：

```bash
mvn test
```

跳过测试打包：

```bash
mvn clean package -DskipTests
```

仅构建指定模块及其依赖：

```bash
mvn -pl <module-name> -am package
```

研发要求：

- 新增公共模型必须有单元测试；
- 新增模块必须有模块装配测试；
- 新增 Action 必须有请求参数校验测试；
- 新增 Stream 处理器必须有资源关闭与异常路径测试。

### 3.3 启动服务

服务启动一般由 `packaging` 模块中的启动脚本或启动入口完成。

典型服务形态：

- DFTP Server：只装配基础协议内核和数据源模块；
- DACP Server：装配 DFTP 内核、Catalog、Cook 等上层能力；
- DACP Proxy：装配 DFTP 内核、Proxy 转发模块和客户端连接能力。

启动前需要确认：

- host / port 配置正确；
- 模块 XML 或模块列表配置正确；
- 数据源根目录或后端地址可访问；
- ticket TTL、清理周期等运行参数符合测试场景；
- 日志目录可写。

## 4. 核心运行机制

### 4.1 DftpServer 生命周期

`DftpServer` 是服务端容器。典型生命周期：

1. 加载服务端配置；
2. 创建 `ServerContext`；
3. 初始化 `KernelModule`；
4. 加载并初始化业务模块；
5. 收集 Authentication / Action / GetStream / PutStream 处理器；
6. 启动 Arrow Flight Server；
7. 对外接收认证、Action、GetStream、PutStream 请求；
8. 服务关闭时释放 ticket、线程池、连接池、临时目录等资源。

研发约束：

- `DftpServer` 不应直接依赖 Catalog/Cook/Proxy 的业务实现；
- 服务启动失败必须输出明确错误；
- 模块初始化失败时默认快速失败，避免服务处于半可用状态；
- 服务关闭时必须有统一资源清理路径。

### 4.2 KernelModule 路由机制

`KernelModule` 是协议路由内核，负责聚合并路由四类处理器：

- `AuthenticationMethod`：处理认证；
- `ActionMethod`：处理控制面请求；
- `GetStreamMethod`：处理数据面读取；
- `PutStreamMethod`：处理数据面写入。

处理器由各模块注册，`KernelModule` 不关心具体业务，只根据协议请求进行匹配和分发。

路由要求：

- 认证链应有确定顺序；
- Action 应按 actionType 精确匹配；
- GetStream 应按 ticket 精确定位已注册的数据流；
- PutStream 在 `acceptPut` 阶段先按上传 ticket 找到上传参数并构造 `DftpPutStreamRequest`，随后由 `PutStreamMethod.accepts(request)` 选择处理器；
- 未命中处理器时返回标准错误；
- 处理器冲突应在启动阶段尽量发现并告警。

### 4.3 控制面与数据面分离

DFTP-DACP 采用控制面与数据面分离：

- 控制面：Action 请求，负责参数协商、权限检查、任务提交、ticket 注册；
- 数据面：GetStream / PutStream 请求，负责传输 Arrow RecordBatch 或 Blob 分片。

开发要求：

- Action 不直接返回大数据；
- 大结果统一注册为 ticket；
- 客户端通过 ticket 拉取或推送流；
- Proxy 对结果型请求必须执行 ticket 重注册；
- traceId 链路追踪属于规划能力，落地后应覆盖控制面和数据面，便于串联排障。

### 4.4 Ticket 生命周期

ticket 是控制面与数据面之间的桥梁。

典型流程：

1. Action 创建或获取 `DataFrameHandle` / `BlobHandle`；
2. 服务端注册 `ticket -> handle`；
3. Action 返回 ticket 和元信息；
4. 客户端用 ticket 发起 GetStream / PutStream；
5. 服务端根据 ticket 找到 handle；
6. 流结束、过期或失败后清理资源。

研发要求：

- ticket 必须有 TTL；
- ticket 查询失败应返回 `DATA_NOT_FOUND` 或等价错误；
- ticket 不能泄露后端内部地址；
- Proxy 不能把后端 ticket 直接暴露给外部客户端；
- ticket 清理不能阻塞数据面主链路；
- ticket 相关日志应预留链路追踪字段；traceId 机制落地后应写入该字段。

## 5. Module 扩展开发指南（核心）

### 5.1 Module 扩展模型

Module 是 DFTP-DACP 的核心扩展单元。新增能力时，应优先新增模块，而不是修改协议内核。

模块化扩展模型由三部分组成：

- `DftpModule`：模块入口，负责初始化和注册扩展点；
- `EventHandler` / `EventSource`：模块之间收集扩展点的事件机制；
- `KernelModule`：统一收集四类处理器并进行路由。

开发原则：

- 内核稳定，能力可插拔；
- 模块只声明和实现自己的能力；
- 模块之间通过接口、事件或协议交互；
- 不允许模块直接改写内核路由语义；
- 不允许业务模块绕过 `KernelModule` 直接处理协议请求。

### 5.2 DftpModule 接口约定

每个模块应实现 `DftpModule`，并在初始化阶段通过 `Anchor` 挂接 `EventHandler` / `EventSource`。当前代码中模块不是直接把 `ActionMethod`、`GetStreamMethod` 等处理器注册到 `Anchor`，而是通过跨模块事件完成“收集式注册”。

示例结构：

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

如果一个模块内部还需要收集其他模块暴露的能力，可以同时挂接 `EventSource`，在所有模块初始化完成后由 `Modules` 统一触发：

```scala
anchor.hook(new EventSource {
  override def init(eventHub: EventHub): Unit = {
    eventHub.fireEvent(CollectGetStreamMethodEvent(getMethods))
  }
})
```

模块初始化可按需包含：

- 保存 `ServerContext` 引用；
- 创建本模块提供的 `AuthenticationMethod` / `ActionMethod` / `GetStreamMethod` / `PutStreamMethod`；
- 通过 `anchor.hook(eventHandler)` 挂接事件处理器；
- 如模块需要主动收集其他模块能力，再通过 `anchor.hook(eventSource)` 挂接事件源；
- 如模块依赖配置，则读取配置并校验必填项；
- 如模块依赖外部资源，则初始化服务对象、连接池、线程池或缓存；
- 输出模块加载日志。

当前内置收集事件包括：

- `CollectAuthenticationMethodEvent`：收集 `AuthenticationMethod`；
- `CollectActionMethodEvent`：收集 `ActionMethod`；
- `CollectPutStreamMethodEvent`：收集 `PutStreamMethod`；
- `CollectGetStreamMethodEvent`：收集 `GetStreamMethod` 和 `GetStreamFilter`。

收集流程：

1. 所有模块执行 `init(anchor, serverContext)`；
2. 模块通过 `anchor.hook(...)` 挂接自己的 `EventHandler` 和 `EventSource`；
3. 所有模块初始化完成后，`Modules` 依次初始化已挂接的 `EventSource`；
4. `EventSource` 通过 `EventHub.fireEvent(...)` 发出收集事件；
5. 匹配该事件的 `EventHandler` 将自身能力加入事件携带的 collector；
6. `KernelModule` 使用收集到的 handler 完成认证、Action、GetStream、PutStream 路由。

接口约束：

- `init` 必须幂等；
- `init` 不应执行长时间阻塞任务；
- `init` 失败必须抛出明确异常；
- 模块不得吞掉初始化异常后继续运行；
- 模块应避免在构造函数中做复杂初始化；
- `EventHandler.accepts` 应尽量精确匹配事件类型，避免无关事件进入处理逻辑；
- `ActionMethod.accepts` 不建议无条件返回 `true`，除非该模块就是兜底处理器，否则会抢占后续 Action 处理器。

### 5.3 Module 生命周期

新增模块从开发到运行的生命周期：

1. 设计模块职责；
2. 定义输入输出契约；
3. 实现 `DftpModule`；
4. 实现对应处理器；
5. 在 `init` 中通过 `Anchor` 挂接 `EventHandler` / `EventSource`；
6. 添加配置项；
7. 添加 XML 或启动装配配置；
8. 编写单元测试；
9. 编写模块装配测试；
10. 启动服务验证；
11. 补充日志、指标和排障说明；
12. 进入发行包。

模块关闭阶段应释放：

- 线程池；
- 连接池；
- 文件句柄；
- 临时目录；
- 本地缓存；
- 后端客户端连接；
- 未完成 ticket 关联资源。

### 5.4 Module 装配方式

当前实现通过 Spring XML 的 `modules` 列表完成模块装配。模块由 Spring 创建为 `DftpModule` bean，并注入到 `DftpServerConfigBean.modules`。

当前已实现字段：

- `class`：模块实现类，由 Spring bean 的 `class` 属性声明；
- 构造参数：通过 `<constructor-arg>` 注入；
- Java/Scala Bean 属性：通过 `<property>` 注入。

当前未实现的装配元数据：

- `module.enabled`：模块级启停字段尚未由框架统一处理；
- `module.order`：显式 order 字段尚未实现；
- `module.configRef`：配置前缀引用字段尚未实现。

当前模块加载顺序由 XML `<list>` 中 bean 的声明顺序决定。

示例：

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

装配要求：

- class 不存在时启动失败；
- 模块构造失败时启动失败；
- 模块初始化失败时启动失败；
- 如需禁用模块，当前应从 XML 的 `modules` 列表中移除或注释对应 bean；
- 如模块内部自行实现 `enabled` 配置，应在 `init` 中判断并避免挂接 handler；
- 模块加载顺序应以 XML 列表顺序为准。

### 5.5 Module 加载顺序与冲突处理

当前加载顺序：

- `DftpServer` 会先创建内部 `KernelModule`；
- XML 中声明的模块会按 `modules` 列表顺序加入 `Modules`；
- `buildServer()` 阶段会追加内部 `KernelModule`，随后执行 `modules.init()`；
- `modules.init()` 先按加入顺序执行每个模块的 `init(anchor, serverContext)`，再按挂接顺序初始化所有 `EventSource`。

XML 中的模块声明顺序建议：

1. 基础内核模块；
2. 认证模块；
3. 数据源模块；
4. Catalog 模块；
5. Cook 模块；
6. Proxy 模块；
7. AccessLog / Metrics 等观测模块。

冲突处理规则：

- Authentication：按注册顺序或显式优先级匹配，首个可处理的认证方式生效；
- Action：按 actionType 精确匹配，重复注册时启动阶段告警或失败；
- GetStream：按 ticket 精确匹配已注册的 DataFrame / Blob 流；
- PutStream：上传 ticket 只用于在 `acceptPut` 阶段恢复上传参数，进入 `KernelModule` 后由 `PutStreamMethod.accepts(request)` 匹配处理器；
- Proxy：重封装类 Action 应明确声明覆盖策略。

建议首版策略：

- 同一 actionType 不允许多个模块同时注册；
- 同一 ticket 不应被多个 Stream 处理器同时处理；
- 冲突清单在启动日志中输出；
- 生产环境默认对严重冲突快速失败。

规划能力：

- 增加显式 `module.order`；
- 增加统一 `module.enabled`；
- 增加启动阶段冲突检测；
- 增加模块配置前缀 `module.configRef`。

### 5.6 Module 初始化约束

模块初始化必须满足：

- 幂等：重复初始化不得重复注册处理器；
- 可诊断：失败信息必须包含模块名、配置前缀、失败原因；
- 可控：不得在初始化阶段启动无限循环或长期阻塞任务；
- 隔离：不得直接修改其他模块私有状态；
- 最小权限：只读取自身配置和必要上下文；
- 可清理：初始化部分成功后失败，必须释放已创建资源。

禁止行为：

- 绕过 `KernelModule` 直接处理 Flight 请求；
- 在模块间强行类型转换访问私有实现；
- 静态全局缓存持有用户凭据；
- 在日志中打印密码、token、私钥；
- 在 Action 中直接返回大规模数据。

### 5.7 Module 配置规范

配置 key 建议使用模块前缀：

```text
dftp.*
dacp.*
catalog.*
cook.*
proxy.*
ticket.*
accesslog.*
```

每个模块应在开发文档或 README 中列出配置矩阵：

| 配置项 | 类型 | 默认值 | 是否必填 | 说明 |
|---|---|---:|---|---|
| `xxx.enabled` | boolean | `true` | 否 | 是否启用模块 |
| `xxx.timeoutMs` | long | `30000` | 否 | 请求超时时间 |
| `xxx.rootDir` | string | 无 | 是 | 模块数据根目录 |

配置读取要求：

- 必填项缺失时启动失败；
- 非法值应在启动阶段报错；
- 默认值应显式声明；
- 敏感配置不得打印明文；
- 配置项变更影响应写入文档。

### 5.8 Module 资源管理

模块如持有外部资源，必须定义清理策略。

常见资源：

- 数据库连接池；
- 对象存储客户端；
- 远端 DFTP/DACP 客户端；
- 线程池；
- 本地缓存；
- 临时文件；
- 执行器工作目录；
- ticket 关联的 DataFrame / Blob handle。

开发要求：

- 资源创建和释放路径必须成对出现；
- Stream 读取中断时必须关闭底层 reader；
- PutStream 上传失败时必须清理临时文件；
- Cook 作业失败时必须释放执行资源；
- Proxy 后端连接应有超时、重试和连接池上限；
- 清理失败应记录日志，但不得影响主清理流程继续执行。

### 5.9 Module 日志与观测

模块日志应覆盖：

- 模块加载；
- 配置摘要；
- 处理器注册；
- Action 请求进入和返回；
- Stream 开始和结束；
- ticket 注册和清理；
- 作业状态变化；
- 外部依赖调用；
- 异常和超时。

日志要求：

- 当前实现尚未提供统一 `traceId` 机制；
- 规划中所有请求链路应携带 `traceId`；
- traceId 落地后，Action 和 Stream 日志应可通过 traceId 关联；
- 敏感信息必须脱敏；
- 错误日志保留服务端堆栈；
- 对外响应只返回必要错误信息。

建议指标：

- Action 请求数、失败数、耗时；
- Stream 活跃数量、吞吐、字节数；
- ticket 数量、过期数量；
- Cook 作业数量、失败数、平均耗时；
- Proxy 后端请求数、重试数、超时数；
- 模块初始化耗时。

### 5.10 Module 测试要求

每个新增模块至少包含：

- 模块初始化测试；
- 配置缺失测试；
- 配置非法值测试；
- 处理器注册测试；
- 与 `KernelModule` 的路由测试；
- 正常请求测试；
- 异常请求测试；
- 资源释放测试。

模块测试 Checklist：

- 模块可被 XML 装配；
- disabled 后不会注册处理器；
- actionType 不冲突；
- ticket 可注册、查询、过期；
- Stream 中断后资源关闭；
- 错误码符合规范；
- 如模块实现了 traceId，则日志包含 traceId；
- 敏感字段不进入日志。

## 6. 扩展点开发指南

### 6.1 Authentication 扩展

Authentication 扩展用于新增认证方式。

适用场景：

- 匿名认证；
- 用户名密码认证；
- token 认证；
- keypair 认证；
- 外部 IdP 映射。

实现要求：

- 实现 `AuthenticationMethod`；
- 判断当前 credentials 是否由本认证方式处理；
- 校验凭据；
- 生成 `UserPrincipal`；
- 将身份信息注入后续请求上下文；
- 认证失败返回标准 `AUTH_*` 错误。

处理流程：

1. 客户端提交 credentials；
2. `KernelModule` 遍历认证处理器；
3. 匹配到处理器后执行校验；
4. 校验成功后返回用户身份；
5. 后续 Action / Stream 使用该身份做权限判断和审计。

开发注意：

- 多个认证模块并存时必须有明确优先级；
- token、密码、私钥不得进入日志；
- 认证模块应避免依赖业务模块；
- 外部 IdP 不可用时应返回可诊断错误。

### 6.2 Action 扩展

Action 扩展用于新增控制面能力。

适用场景：

- GET 参数协商；
- PUT 上传协商；
- Catalog 查询；
- Cook 作业提交；
- Job 状态查询；
- Proxy 转发；
- 管理类请求。

实现要求：

- 实现 `ActionMethod`；
- 定义唯一 actionType；
- 校验请求参数；
- 执行权限检查；
- 返回 JSON 或 ticket 元信息；
- 发生错误时返回统一错误模型。

actionType 命名建议：

```text
CATALOG_LIST_DATASETS
CATALOG_GET_SCHEMA
COOK_SUBMIT_RECIPE
COOK_SUBMIT_FLOW
COOK_GET_JOB_STATUS
COOK_GET_JOB_EXECUTE_RESULT
PROXY_FORWARD_ACTION
```

Action 响应要求：

- 小结果可直接返回 JSON；
- 大结果必须注册 ticket；
- traceId 属于规划能力；落地后 Action 响应应包含 traceId；
- 错误响应必须包含稳定错误码；
- 不得返回服务端内部路径、后端 ticket 或敏感配置。

### 6.3 GetStream 扩展

GetStream 扩展用于新增数据读取能力。

适用场景：

- 读取本地文件；
- 读取对象存储；
- 读取数据库；
- 读取缓存；
- 读取 Cook 结果；
- 通过 Proxy 读取后端结果。

实现要求：

- 实现 `GetStreamMethod`；
- 根据 ticket 精确定位已注册的数据流；
- 以 Arrow RecordBatch 或等价分片方式流式输出；
- 支持客户端提前中断；
- 结束时关闭底层资源；
- 异常时返回标准错误。

开发注意：

- 不得一次性加载大数据；
- RecordBatch 大小应可配置；
- 数据源 reader 必须在 finally 或等价路径释放；
- ticket 不存在时返回 `DATA_NOT_FOUND`；
- 权限不足时返回 `PERM_ACCESS_DENIED`；
- Blob 可复用统一流机制承载。

### 6.4 PutStream 扩展

PutStream 扩展用于新增数据写入能力。

适用场景：

- 上传 DataFrame；
- 上传 Blob；
- 写入文件系统；
- 写入对象存储；
- 写入数据库；
- 写入 Catalog 管理的数据版本。

典型流程：

1. 客户端发起 Action 请求上传；
2. 服务端校验目标、权限、schema 和大小限制；
3. 服务端创建上传 ticket；
4. 客户端通过 PutStream 推送数据；
5. 服务端消费流并写入目标；
6. 写入完成后返回资源标识或版本信息。

开发要求：

- 上传前必须做参数校验；
- 上传过程中必须支持异常中断；
- 上传失败必须清理临时数据；
- 目标已存在时返回 `DATA_ALREADY_EXISTS` 或版本冲突错误；
- 写入成功后应产出可复取标识；
- PUT 与 Catalog 版本管理应有清晰边界。

### 6.5 Catalog 扩展

Catalog 扩展用于接入新的目录、元数据和数据发现能力。

扩展入口：

- `CatalogService`；
- `CatalogFormatter`；
- Catalog 相关 `ActionMethod`。

可扩展内容：

- 数据集发现；
- 数据帧发现；
- Schema 查询；
- 统计信息；
- 文档信息；
- RDF / JSON-LD 元数据格式；
- PUT 后版本管理；
- lineage 与复现信息。

开发要求：

- Catalog 查询不应直接读取大规模数据；
- Schema 和统计信息可以按需计算或预计算；
- 元数据字段新增应向后兼容；
- 外部目录不可用时返回 `SYS_BACKEND_UNAVAILABLE`；
- Catalog 结果应避免暴露内部存储路径。

### 6.6 Cook 扩展

Cook 扩展用于新增算法执行、Flow 调度和算子仓库接入能力。

扩展方向：

- 新增 `TransformOp`；
- 新增 Flow 节点；
- 新增执行器；
- 接入 Repository Operator；
- 扩展 Job 状态；
- 增加资源配额、重试、缓存和隔离策略。

Recipe / Flow 开发要求：

- 输入参数必须可校验；
- 输出 schema 应尽量预声明；
- 作业必须有 `jobId`；
- 作业状态必须可查询；
- 执行结果必须注册 ticket；
- 作业失败必须保留可诊断错误；
- 不应默认返回原始大数据。

执行器开发要求：

- 执行环境与服务端主进程隔离；
- 工作目录按 job 隔离；
- 资源配额可配置；
- 超时可配置；
- 依赖缓存按版本或哈希隔离；
- 用户代码不得持有超出授权范围的数据访问能力。

算子仓库接入要求：

- 支持固定版本；
- 生产环境不建议默认使用 latest；
- 参数必须按 schema 校验；
- artifact 拉取失败应快速失败；
- 本地缓存应有 TTL 和容量限制；
- 作业记录应保留 operatorId / operatorVersion。

### 6.7 Proxy 扩展

Proxy 扩展用于协议级转发与后端拓扑隐藏。

扩展方向：

- 透明转发；
- 按资源路由；
- 按用户路由；
- 负载均衡；
- Action 重封装；
- ticket 重注册；
- 后端连接池；
- 超时、重试、熔断。

开发要求：

- 对外不得暴露后端地址；
- 对外不得暴露后端 ticket；
- 结果型 Action 必须重注册代理侧 ticket；
- traceId 属于规划能力；落地后下游错误应保留原始 traceId；
- traceId 属于规划能力；落地后代理侧应追加 proxyTraceId；
- 后端凭据和客户端凭据应隔离；
- Proxy 日志应能串联客户端请求和后端请求。

## 7. 新模块开发完整示例

本章以 `AccessLogModule` 为示例，说明一个模块从创建到装配的完整流程。

### 7.1 示例目标

`AccessLogModule` 目标：

- 记录 Action 请求；
- 记录 GetStream / PutStream 开始与结束；
- 记录请求用户、请求类型、资源标识、耗时、行数、字节数和错误码；
- 支持配置开关；
- 不阻塞主请求链路。

### 7.2 创建 Maven 子模块

建议模块名：

```text
access-log-module
```

依赖建议：

- 依赖 `dftp-common` 获取公共模型；
- 依赖 `dftp-server` 获取模块接口和服务端上下文；
- 不依赖 `catalog-module`、`cook-module` 或 `dacp-proxy` 的私有实现。

### 7.3 实现 Module 类

示例：

```java
public class AccessLogModule implements DftpModule {
    @Override
    public void init(ModuleAnchor anchor, ServerContext serverContext) {
        AccessLogConfig config = AccessLogConfig.from(serverContext);
        AccessLogWriter writer = new AsyncAccessLogWriter(config);

        anchor.registerEventHandler(new AccessLogEventHandler(writer));
    }
}
```

实现重点：

- 从配置中读取是否启用；
- 初始化异步日志 writer；
- 注册事件处理器；
- 服务关闭时释放 writer。

### 7.4 实现 Handler

Handler 应处理：

- Action 请求开始；
- Action 请求结束；
- Stream 请求开始；
- Stream 请求结束；
- 请求异常；
- 作业状态变化。

规划中的日志字段建议：

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

### 7.5 添加配置项

示例：

```text
accesslog.enabled=true
accesslog.async=true
accesslog.queueSize=10000
accesslog.output=file
accesslog.file.path=logs/access.log
```

要求：

- `queueSize` 必须有上限；
- 日志写入失败不得影响数据面；
- 关闭服务时应尽量 flush 剩余日志。

### 7.6 装配模块

示例：

```xml
<bean class="org.example.dacp.accesslog.AccessLogModule"/>
```

`AccessLogModule` 建议放在 XML `modules` 列表靠后位置，以便观察已经注册的业务能力。

### 7.7 编写测试

测试项：

- 模块启用时能注册 handler；
- 模块禁用时不注册 handler；
- Action 请求能生成访问日志；
- Stream 中断时能生成异常日志；
- 敏感字段会脱敏；
- 异步队列满时不阻塞主链路。

## 8. 客户端开发指南

### 8.1 DftpClient 基础能力

`DftpClient` 面向 DFTP 基础能力：

- 建立 Flight 连接；
- 执行认证；
- 发起 Action；
- 通过 ticket 执行 GetStream；
- 通过 ticket 执行 PutStream；
- 将远端 DataFrame 封装为可迭代对象。

客户端要求：

- 连接应支持超时配置；
- 请求应携带认证信息；
- 错误应保留服务端错误码；
- Stream 使用完成后必须关闭；
- 客户端不应假设服务端本地路径结构。

### 8.2 DacpClient 高层 API

`DacpClient` 面向 DACP 上层能力：

- Catalog 查询；
- Schema 查询；
- Cook Recipe 提交；
- Cook Flow 提交；
- Job 状态查询；
- Job 结果读取。

封装原则：

- 高层 API 隐藏 actionType 细节；
- 保留必要的底层错误信息；
- 返回结果应支持惰性读取；
- 不应把大结果 collect 到内存作为默认行为。

### 8.3 RemoteDataFrameProxy 使用方式

远端 DataFrame 应按流式方式消费。

使用要求：

- 支持迭代读取；
- 支持主动关闭；
- 支持读取中断；
- 支持 schema 预读；
- 不默认缓存全量数据。

### 8.4 客户端错误处理

客户端需要处理：

- 认证失败；
- actionType 不支持；
- 参数非法；
- ticket 不存在或过期；
- 服务端 Stream 中断；
- 后端不可用；
- Cook 作业失败；
- Proxy 下游错误。

错误处理要求：

- 保留 `code`；
- 如服务端返回了 `traceId`，客户端应保留；
- 如 Proxy 场景返回了 `proxyTraceId`，客户端应同时保留；
- 不把所有异常都包装成通用 RuntimeException；
- 可重试错误和不可重试错误应区分。

## 9. 配置开发指南

### 9.1 配置文件结构

首版建议配置分为两类：

- 属性配置：`dftp.conf` / `dacp.conf`；
- 模块装配配置：`dftp.xml` / `dacp.xml`。

属性配置负责运行参数，模块装配配置负责声明加载哪些模块。

### 9.2 配置命名规范

配置 key 应按模块或能力前缀分组：

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

命名要求：

- 使用小写和点分隔；
- 前缀必须能定位模块；
- 时间单位写入 key 名，例如 `timeoutMs`；
- 布尔值使用 `enabled`；
- 不同模块不得复用含义不同的同名 key。

### 9.3 必填配置与默认值

每个模块必须声明：

- 必填配置；
- 可选配置；
- 默认值；
- 取值范围；
- 非法值处理方式；
- 是否允许热更新。

首版建议不支持模块热插拔；如支持部分配置热更新，需要逐项声明。

### 9.4 敏感配置处理

敏感配置包括：

- 密码；
- token；
- 私钥；
- 外部服务凭据；
- 数据库连接凭据；
- 对象存储密钥。

要求：

- 日志中必须脱敏；
- 错误响应不得携带明文；
- 配置 dump 不得输出完整值；
- 测试配置不得提交真实密钥。

## 10. 错误码与异常处理

### 10.1 统一错误模型

Action 错误响应建议统一为以下结构。当前实现尚未统一 `traceId`，该字段属于规划能力：

```json
{
  "code": "REQ_INVALID_ARGUMENT",
  "message": "invalid request argument",
  "traceId": "...",
  "details": {}
}
```

字段说明：

- `code`：稳定错误码，供程序判断；
- `message`：可读错误描述；
- `traceId`：链路追踪 ID，规划字段；
- `details`：机器可解析的附加信息。

### 10.2 错误码分段

建议错误码分段：

- `AUTH_*`：认证失败、凭据缺失、凭据格式错误；
- `PERM_*`：权限不足、操作未授权；
- `REQ_*`：请求参数错误、schema 不匹配、不支持的操作；
- `DATA_*`：数据源不可达、文件不存在、格式不支持、ticket 不存在；
- `JOB_*`：作业不存在、作业失败、作业超时、状态冲突；
- `SYS_*`：系统内部错误、依赖不可用。

### 10.3 Flight 状态映射

建议映射：

| 业务错误码 | HTTP 语义 | Flight 状态 | 场景 |
|---|---:|---|---|
| `AUTH_INVALID_CREDENTIALS` | 401 | `UNAUTHENTICATED` | 凭据错误 |
| `AUTH_MISSING_CREDENTIALS` | 401 | `UNAUTHENTICATED` | 未携带凭据 |
| `PERM_ACCESS_DENIED` | 403 | `UNAUTHORIZED` | 无权限 |
| `DATA_NOT_FOUND` | 404 | `NOT_FOUND` | 数据或 ticket 不存在 |
| `JOB_NOT_FOUND` | 404 | `NOT_FOUND` | 作业不存在 |
| `REQ_INVALID_ARGUMENT` | 400 | `INVALID_ARGUMENT` | 参数非法 |
| `REQ_UNSUPPORTED_OPERATION` | 400 | `INVALID_ARGUMENT` | 操作不支持 |
| `DATA_ALREADY_EXISTS` | 409 | `ALREADY_EXISTS` | PUT 目标冲突 |
| `JOB_CONFLICT` | 409 | `FAILED_PRECONDITION` | 作业状态冲突 |
| `JOB_TIMEOUT` | 408 | `TIMED_OUT` | 作业超时 |
| `SYS_BACKEND_UNAVAILABLE` | 503 | `UNAVAILABLE` | 后端不可用 |
| `SYS_INTERNAL_ERROR` | 500 | `INTERNAL` | 未分类内部错误 |

### 10.4 模块异常处理要求

模块异常处理要求：

- 参数错误在 Action 入口快速失败；
- 权限错误必须在访问数据前失败；
- 外部依赖错误不得包装成参数错误；
- 服务端日志记录完整异常；
- 客户端响应不暴露敏感细节；
- 未分类异常统一归为 `SYS_INTERNAL_ERROR`。

### 10.5 Proxy 错误包装规则

Proxy 转发错误时：

- 保留下游 `code`；
- traceId 机制落地后，保留下游 `traceId`；
- traceId 机制落地后，增加代理侧 `proxyTraceId`；
- 标记后端地址时不得暴露内部敏感拓扑；
- 后端不可达时返回 `SYS_BACKEND_UNAVAILABLE`。

## 11. 测试指南

### 11.1 单元测试

单元测试覆盖：

- 公共模型序列化；
- TransformOp 编解码；
- 参数校验；
- 错误码映射；
- 模块配置解析；
- handler 纯逻辑。

### 11.2 集成测试

集成测试覆盖：

- 模块装配；
- `KernelModule` 路由；
- 认证链；
- Action 调用；
- ticket 注册与查询；
- GetStream / PutStream；
- Catalog 查询；
- Cook 作业状态流转；
- Proxy ticket 重注册。

### 11.3 端到端测试

端到端场景建议至少包含：

1. 启动 DACP Server；
2. 客户端认证；
3. Catalog 查询数据集；
4. GET DataFrame；
5. 提交 Cook Recipe；
6. 查询 Job 状态；
7. 读取 Cook 结果；
8. 通过 Proxy 重复执行 GET 或 Cook 结果读取。

### 11.4 模块扩展测试模板

每个新增模块应验证：

- 可加载；
- 可禁用；
- 配置缺失时失败；
- 配置非法时失败；
- handler 正确注册；
- handler 冲突可发现；
- 请求可被路由；
- 异常路径返回标准错误；
- 资源可释放。

### 11.5 性能测试

性能测试关注：

- RecordBatch 大小；
- 单流吞吐；
- 并发流数量；
- ticket 数量；
- ticket 清理成本；
- Cook 并发作业；
- Proxy 转发开销；
- Catalog 大目录查询耗时。

性能测试输出应包含：

- 测试数据规模；
- 并发度；
- 平均耗时；
- P95 / P99；
- 吞吐；
- CPU / 内存；
- 错误数。

## 12. 打包与部署

### 12.1 packaging 模块说明

`packaging` 负责生成可部署发行包。

职责：

- 聚合服务端 jar；
- 聚合模块依赖；
- 提供启动脚本；
- 提供默认配置；
- 提供示例 XML；
- 输出 DFTP / DACP / Proxy 不同服务形态。

### 12.2 发行包结构

建议发行包结构：

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

### 12.3 启动脚本

启动脚本应支持：

- 指定配置目录；
- 指定日志目录；
- 指定 JVM 参数；
- 前台启动；
- 后台启动；
- 停止服务；
- 查看服务状态。

### 12.4 模块启停

当前模块启停通过 XML 装配控制：需要启用模块时加入对应 bean，需要禁用模块时从 `modules` 列表中移除或注释对应 bean。

要求：

- 未装配的模块不得注册 handler；
- 未装配模块的配置缺失不应影响启动；
- 如果模块自行实现 `enabled` 配置，则禁用时不得注册 handler；
- 被依赖模块禁用时，依赖方应启动失败并说明原因。

### 12.5 生产部署建议

生产环境建议：

- 固定端口和服务名；
- 关闭匿名认证；
- 设置 ticket TTL 和容量上限；
- 设置 Cook 并发上限；
- 设置 Proxy 超时和重试；
- 开启访问日志；
- 日志接入集中检索系统；
- 使用固定版本算子；
- 明确数据目录和临时目录清理策略。

## 13. 可观测与排障

### 13.1 日志规范

当前日志字段以现有实现为准。规划中的结构化日志建议至少包含：

- timestamp；
- level；
- traceId；
- module；
- actionType；
- user；
- resource；
- durationMs；
- errorCode；
- message。

### 13.2 traceId 传递（规划）

当前实现尚未提供统一 traceId 传递机制。规划要求：

- 客户端可传入；
- 服务端缺失时生成；
- Action 响应返回；
- Stream 日志携带；
- Cook Job 记录携带；
- Proxy 转发时保留下游 traceId 并增加 proxyTraceId。

### 13.3 AccessLogModule 建议

访问日志建议独立模块实现，不侵入业务模块。

记录时机：

- Action 请求进入；
- Action 请求结束；
- GetStream 开始；
- GetStream 结束；
- PutStream 开始；
- PutStream 结束；
- Job 提交；
- Job 状态变化；
- Job 结果读取。

### 13.4 常见问题

模块未加载：

- 检查 XML 中 class 是否正确；
- 检查 jar 是否在 `lib/`；
- 检查模块 bean 是否已加入 `modules` 列表；
- 如果模块自行实现 `enabled` 配置，检查该配置是否为 `true`；
- 检查启动日志中的模块加载顺序。

Action 未路由：

- 检查 actionType 是否一致；
- 检查 ActionMethod 是否注册；
- 检查是否存在 actionType 冲突；
- 检查 KernelModule 是否完成处理器收集。

ticket 不存在：

- 检查 Action 是否成功注册 ticket；
- 检查 ticket 是否过期；
- 检查 Proxy 是否完成 ticket 重注册；
- 检查客户端是否访问了错误服务节点。

Stream 中断：

- 检查客户端是否提前关闭；
- 检查数据源 reader 是否异常；
- 检查网络连接；
- 检查 RecordBatch 大小；
- 检查服务端错误日志。

Cook 作业失败：

- 检查 Recipe / Flow 参数；
- 检查输入数据权限；
- 检查执行器工作目录；
- 检查资源配额；
- 检查算子版本和依赖；
- 检查 Job 错误详情。

Proxy 请求失败：

- 检查后端地址；
- 检查后端认证；
- 检查连接池和超时；
- 如已实现 traceId，检查下游 traceId；
- 检查代理侧 ticket 重注册。

## 14. 开发规范

### 14.1 代码风格

要求：

- 命名清晰；
- 方法职责单一；
- 避免跨模块私有实现依赖；
- 公共模型保持稳定；
- 异常处理明确；
- 测试覆盖核心路径。

### 14.2 命名规范

建议：

- 模块类以 `Module` 结尾；
- Action 处理器以 `ActionMethod` 或 `ActionHandler` 结尾；
- Stream 处理器以 `GetStreamMethod` / `PutStreamMethod` 结尾；
- 配置类以 `Config` 结尾；
- 服务类以 `Service` 结尾；
- 客户端类以 `Client` 结尾。

### 14.3 Action 命名规范

Action 名称应：

- 全大写；
- 下划线分隔；
- 模块名前缀开头；
- 表达动作语义；
- 一经发布保持稳定。

示例：

```text
CATALOG_LIST_DATASETS
CATALOG_GET_DATAFRAME_SCHEMA
COOK_SUBMIT_FLOW
COOK_GET_JOB_STATUS
```

### 14.4 兼容性规范

兼容性要求：

- 新字段优先追加；
- 不改变旧字段含义；
- 不复用旧错误码表达新语义；
- 删除字段必须经过废弃周期；
- 协议行为变化必须写入发布说明；
- 生产环境算子建议固定版本。

### 14.5 安全规范

安全要求：

- 默认不打印敏感信息；
- 认证和授权分层处理；
- Cook 执行应遵循最小权限；
- 用户代码应与服务端主进程隔离；
- Proxy 凭据应与客户端凭据隔离；
- PUT 目标路径必须校验，避免越权写入；
- GET 资源路径必须校验，避免越权读取。

## 15. 附录

### 15.1 核心接口清单

核心接口：

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

### 15.2 Module 开发 Checklist

新增模块交付前确认：

- 已定义模块职责；
- 已定义输入输出契约；
- 已实现 `DftpModule`；
- 已注册扩展点；
- 已添加配置项；
- 已添加 XML 装配示例；
- 已处理初始化失败；
- 已处理资源释放；
- 已添加单元测试；
- 已添加装配测试；
- 已添加异常路径测试；
- 已补充日志；如实现 traceId，则已验证 traceId 传递；
- 已处理敏感信息脱敏；
- 已更新开发文档。

### 15.3 Action 开发 Checklist

新增 Action 交付前确认：

- actionType 唯一；
- 请求参数已校验；
- 权限已检查；
- 大结果通过 ticket 返回；
- 错误码符合规范；
- 如实现 traceId，则响应包含 traceId；
- 已添加正常路径测试；
- 已添加参数错误测试；
- 已添加权限错误测试。

### 15.4 Stream 开发 Checklist

新增 Stream 处理器交付前确认：

- ticket 查询逻辑正确；
- 数据按批流式传输；
- 客户端中断可处理；
- 底层资源可关闭；
- 大小和批次参数可配置；
- 错误映射正确；
- 如实现 traceId，则日志包含 traceId；
- 已添加大数据场景测试；
- 已添加中断场景测试。

### 15.5 配置项模板

```text
<module>.enabled=true
<module>.timeoutMs=30000
<module>.maxConcurrency=16
<module>.cache.ttlSeconds=3600
<module>.log.enabled=true
```

### 15.6 发布前检查

发布前确认：

- 全量测试通过；
- 端到端场景通过；
- 默认配置可启动；
- 模块冲突检查通过；
- 启动脚本可用；
- 文档与配置一致；
- 错误码表已更新；
- 版本变更已记录。
