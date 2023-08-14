# Apache Arrow

Apache Arrow 最开始是作为列式数据的内存规范，并以 Java 和 C++ 进行实现。这种内存格式对于支持 SIMD (单指令，多数据) 的 CPU 和 GPU 等现代硬件的矢量化处理是非常有效率的的。

采用标准化的数据内存格式有以下几个好处：

- 如 Python 或 Java 等高级语言可以通过传递数据指针来调用 Rust 或 C++ 等低级语言来完成计算密集型任务，而不是以另一种格式复制数据，这样造成的开销会非常大。

- 由于内存格式也是网络传输格式 (尽管数据也能被压缩)，数据可以在进程之间有效的地传输，而不需要太多的序列化开销。

- 它应该能让数据科学和数据分析领域的各种开源和商业项目之间构建连接器、驱动程序和集成变得更加容易，并允许开发人员使用他们偏好的语言来利用这些平台。

Apache Arrow 现在在许多编程语言中都有实现，包括 C、C++、C#、Go、Java、JavaScript、Julia、MATLAB、Python、R、Ruby 和 Rust。

## Arrow 内存模型

在 [Arrow](https://arrow.apache.org/docs/format/Columnar.html) 的网站上详细描述了这一内存模型，但实际上每一个列都是由单个向量表示，其中包含原始数据，以及表示空值的独立向量和可变宽度类型的原始数据偏移量。

## Inter-Process Communication (IPC) 程间通讯

正如之前所提，可以通过指针在进程和进程之间传递数据。然而，接收进程需要知道如何解析这些数据，因此交换元数据 (如 schema 结构信息) 定义了 IPC 的数据格式。Arrow 使用 Google Flatbuffers 进行元数据格式定义。

## Compute Kernels 计算内核

Apache Arrow 的范围已经扩展到提供计算库来评估数据表达式。Java、C++、C、Python、Ruby、Go、Rust 和 JavaScript 实现等都包含了用于在 Arrow 内存块上执行计算的计算库。

由于这本书主要涉及 Java 实现，值得指出的是，Dremio 最近贡献了 Gandiva 项目，这是一个 Java 库，可以将表达式编译为 LLVM，并支持 SIMD。JVM 开发者可以将操作委托给 Gandava 库，并从中获得纯 Java 实现中难以企及的性能提升 。

## Arrow Flight Protocol

最近，Arrow 已经定义了一个名为 [Flight](https://arrow.apache.org/docs/format/Flight.html) 的协议，以便在网络上高效地传输 Arrow 数据。Flight 基于 [gRPC](https://grpc.io/) 和 [Google Protocol Buffers](https://protobuf.dev/).

Flight 协议由以下方法定义了一个 FlightService:

> 译者建议：在阅读下文之前先对 Google Protobuf 和 gRPC 中的 Service、Client Side 接口和 Stream 有所了解。
> 
> [what is grpc](https://grpc.io/docs/what-is-grpc/introduction/)
 
### Handshake

客户端与服务端之间的握手。根据服务器的不同，可能需要握手来决定用于未来操作的 Token。根据验证机制，请求和响应应都是允许多次往返的数据流 (gRPC Stream)。

### LightFlights

获取给定特定条件下的可用流列表。大多数 Flight 服务都会公开一个或多个流。这个 API 允许列出可用的流。用户也可以提供一套标准，用于限制通过该接口列出的流的子集。每个 Flight 服务都允许自己定义如何使用标准。

### GetFlightInfo

对于给定的 FilgthDescriptor，获取有关 Flight 可以如何被消费的信息。如果接口的使用者已经可以识别要消费的特定 Flight，那这将会是一个非常有用的接口。该接口还允许消费者通过指定的描述符生成 Flight 流。例如：一个 Flight 描述符可能包含待执行的 SQL 语句或[序列化的 Python 操作](https://docs.python.org/3/library/pickle.html)。在这些情况下，之前在 ListFilghts 可用流列表中未提供的流，反而在特定的 Flight 服务定义期间是可用的。

### GetSchema

对于给定的 FlightDescriptor，获取 Schema.fbs::Schema 中描述的 Schema。该接口被用于当消费者需要 Flight 流的 Schema 时。与 GetFlightInfo 类似，该接口可以生成一个之前在 ListFlights 可用列表中未列出的新的 Flight。

### DoGet

检索与引用 ticket 相关的特定描述符所关联的单个流。Flight 可以由一个或多个数据流组成，其中每个数据流都可以使用单独的 opaque ticket 不透明凭证进行检索，Flight 服务使用该 ticket 管理数据流的集合。

### DoPut

将流推送到与特定 Flight 流相关的 Flight 服务。这允许 Flight 服务的客户端上传数据流。根据特定的 Flight 服务，可以允许客户端消费者上传每个描述符的单个流，也可以上传数量不限的流。后者，服务可能会实现一个 "seal" 动作，一旦所有的流被上传，这个动作就可以应用到一个描述符上。

### DoExchange

为指定描述符打开双向数据通道。这允许客户端在单个逻辑流中发送和接收任意 Arrow 数据和特定应用程序的元数据。与 DoGet/DoPut 不同的是，这样操作更适合将计算（而非存储）转移到 Flight 服务的客户端。

### DoAction

除了可能提供的 ListFlights、GetFlightInfo、DoGet 和 DoPut 操作外，Flight 服务还可以支持任意数量的简单操作。DoAction 允许 Flight 客户端对 Flight 服务执行特定事件。一个事件包括 opaque request 匿名请求和响应对象，这些对象与所执行的事件类型有关。

### ListActions

Flight 服务会暴露出所有可用的事件类型及其说明。这可以让不同的 Flight 消费者了解到 Flight 服务所提供的功能。

## Arrow Flight SQL

有人提议为 Arrow Flight 添加 SQL 功能。在撰写本报告时 (2021 年 1 月)，有一份 C++ 实现的 PR，跟踪 Issue 是 [ARROW-14698](https://github.com/apache/arrow/pull/12616)。

## 查询引擎

### DataFusion

Arrow 的 Rust 实现包含一个名为 DataFusion 的内存查询引擎，该引擎于 2019 年贡献给该项目。该项目正在迅速成熟，并获得了越来越多的关注。例如，InfluxData 正在利用 DataFusion 构建[下一代 InfluxDB 内核](https://www.influxdata.com/glossary/apache-datafusion/)。

### Ballista

Ballista 是一个主要由 Rust 实现、Apache Arrow 支持的的分布式计算平台。它的架构允许其它编程语言 (如 Python、C++ 和 Java) 作为一等公民获得支持，而无需考虑序列化开销。

Ballista 的技术基础如下：

- Apache Arrow 用于内存模型和类型系统
- Apache Arrow Flight 协议，用于在进程间高效传输数据
- Apache Arrow Flight SQL 协议，用于商业智能工具和 JDBC 驱动程序连接 Ballista 集群
- Google Protocol Buffers，用于序列化查询计划、
- Docker 用于打包执行器和用户自定义代码
- Kubernetes 用于部署和管理执行器所在的 docker 容器

Ballista 于 2021 年贡献给 Arrow 项目，目前还不能用于生产，不过它能够以良好的性能运行主流 TPC-H 基准中的许多查询案例。

### C++ Query Engine

新增的 C++ 版本查询引擎正在实现中，当下的重点是实现高效计算和数据集 API。