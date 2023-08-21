# Distributed Query Execution 执行分布式查询

上一节关于并行查询执行的内容涵盖了一些基本概念，例如分区，本节将在此基础上进行构建。

为了稍微简化执行分布式查询的概念，我们的目标是创建一个物理查询计划，该计划定义如何将工作分配给集群中的许多 “执行者”。分布式查询计划通常包含新的运算符，这些运算符描述了在请求执行期间数据是如何在不同时间点上与不同的执行者之间进行交换的。

在下面的部分中，我们将深入探讨如何在分布式环境中执行不同类型的计划，然后讨论如何构建分布式查询调度器。

## Embarrassingly Parallel Operators 令人尴尬的并行运算符

在分布式环境下运行时，某些运算符可以在数据分区上并行运行，而不会产生任何显著的开销。最好的例子就是映射和过滤。这些运算符可以并行地应用于正在操作数据的每个输入分区，并为每个输入分区生成相应的输出分区。这些运算符不会改变数据分区方案。

![分布式项目过滤器](https://howqueryengineswork.com/resources/distributed_project_filter.png)

## Distributed Aggregates 分布式聚合

让我们使用在上一章节执行并行查询中所用的 SQL 查询示例，并观察聚合查询在分布式计划中的含义。

```SQL
SELECT passenger_count, MAX(max_fare)
FROM tripdata
GROUP BY passenger_count
```

我们可以在 `tripdata` 表的所有分区上并行执行此查询，集群中的每个执行处理器负责这些分区的一部分。但是，我们需要将所有的结果聚合数据合并到单个节点上，才能应用最终的聚合查询，以便获得一个没有重复的分组键（本例中为 `passenger_count`） 的结果集合。下面是一个可能代表这种情况的逻辑查询计划。注意，新的 `Exchange` 操作符表示执行器之间的数据交换。交换的物理计划可以通过将中间结果写入共享存储来实现，或者可以通过将数据直接以流的形式传输到其它执行器来实现。

```SQL
HashAggregate: groupBy=[passenger_count], aggr=[MAX(max_fare)]
  Exchange:
    HashAggregate: groupBy=[passenger_count], aggr=[MAX(max_fare)]
      Scan: tripdata.parquet
```

下图展示了如何在分布式环境中执行此查询：

![分布式聚合](https://howqueryengineswork.com/resources/distributed_agg.png)

## Distributed Joins 分布式连接

连接通常是在分布式环境中执行的最昂贵的操作。这样说的原因是，我们需要确保在组织数据时，两个输入关系都根据连接键进行了分区。例如，如果我们把 `customer` 表连接到 `order` 表，其中连接的条件是 `customer.id = order.customer_id`，则两个表中针对特定客户的所有行必须由同一执行器处理。要实现这一点，我们必须受限对连接键上的两个表进行重新分区，并将分区写入磁盘。一旦完成，我们就可以对每个分区并行地执行连接了。结果数据仍将按连接键进行分区。这种特殊的连接算法称为分区哈希连接。重新划分数据地过程称为执行 "shuffle"。

![分布式连接](https://howqueryengineswork.com/resources/distributed_join.png)

## Distirbuted Query Scheduling 分布式查询调度

分布式查询计划与进程内查询计划有本质上地不同，因为我们不能仅仅构建一个运算符树并开始执行它们。现在的查询需要跨执行器进行协调，这意味着我们现在开始需要构建一个调度器。

在高层次上，分布式查询调度器的概念并不复杂。调度器需要检查整个查询，并将其分解为可以单独主席那个的阶段（通常跨执行器并行执行），然后根据集群中的可用资源调度执行这些阶段。一旦每个查询阶段完成，就可以安排任何后续的依赖性查询阶段。直到所有查询都被执行完成前都重复这一过程。

调度器还可以负责管理集群中的计算资源，以便可以根据需要启动额外的执行器来处理查询负载。

在本章节的其余部分，我们将讨论以下主题，即 Ballista 和该项目中实现的设计。

- 生成分布式查询计划
- 序列化查询计划并于执行器交换
- 在执行器之间交换中间结果
- 优化分布式查询

## Producing a Distributed Query Plan 生成分布式查询计划

正如我们在前例中看到的那样，一些运算符可以在输入分区上并行运行，而另一些运算符需要对数据进行重新分区。分区中的这些变动是规划分布式查询的关键。计划中的分区变更有时称为管道中断，分区时的这些变更定义了查询阶段之间的边界。

现在我们将使用下面的 SQL 查询来了解这个过程具体是如何工作的：

```SQL
SELECT customer.id, sum(order.amount) as total_amount
FROM customer JOIN order ON customer.id = order.customer_id
GROUP BY customer.id
```

该查询的（非分布式）物理计划如下所示：

```SQL
Projection: #customer.id, #total_amount
  HashAggregate: groupBy=[customer.id], aggr=[MAX(max_fare) AS total_amount]
    Join: condition=[customer.id = order.customer_id]
      Scan: customer
      Scan: order
```

假设 customer 和 order 表还没有根据 customer id 进行分区，我们将需要调度执行前两个查询阶段，以对该数据进行重新分区。这两个查询阶段可以并行。

```SQL
Query Stage #1: repartition=[customer.id]
  Scan: customer
Query Stage #2: repartition=[order.customer_id]
  Scan: order
```

接下来，我们可以调度连接，它将在两个输入的各个分区并行运行。连接之后的下一个运算符是聚合，它被分成两部分：并行前的聚合，然后是需要变成单个输入分区的最终聚合。我们可以在与连接相同的查询阶段执行此聚合的并行部分。