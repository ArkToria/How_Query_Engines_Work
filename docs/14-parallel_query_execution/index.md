# Parallel Query Execution 执行并行查询

到目前为止，我们一直使用单个线性对单个文件进行查询。但这种方法的可伸缩性不是很好，因为对于较大的文件或多个文件，查询将花费更长的时间来运行。下一步是实现分布式执行查询，以便其可以利用多个 CPU 核心和多台服务器。

分布式执行查询最简单的形式是利用线程在单个节点上使用多个 CPU 核心执行并行查询。

为了便于处理，纽约市出驻车数据集已经进行了分区，因为每年的每个月都有一个 CSV 文件，这意味着 2019 年的数据集有 12 个分区，执行并行查询的最直接的方法是在每个分区使用一个线程并执行相同的查询，然后将结果组合在一起。假设这段代码运行在具有 6 个 CPU 核心的并支持超线程的计算机上。在这种情况下，这十二个查询的执行时间应该与在单个线程上运行其中一个查询的运行时间相同。

下面是一个跨 12 个分区并行运行聚合 SQL 查询的示例。这个例子是使用 Kotlin 的协程实现的，而不是直接使用线程。

> 该实例的源代码可以在 KQuery 的 [Github 仓库]([jvm/examples/src/main/kotlin/ParallelQuery.kt](https://github.com/andygrove/how-query-engines-work/blob/main/jvm/examples/src/main/kotlin/ParallelQuery.kt))中找到。

让我们从一个单线程代码开始，针对一个分区运行一个查询。

```kotlin
fun executeQuery(path: String, month: Int, sql: String): List<RecordBatch> {
  val monthStr = String.format("%02d", month);
  val filename = "$path/yellow_tripdata_2019-$monthStr.csv"
  val ctx = ExecutionContext()
  ctx.registerCsv("tripdata", filename)
  val df = ctx.sql(sql)
  return ctx.execute(df).toList()
}
```

准备好后，我们现在可以编写以下代码，以便并行在 12 个数据分区中的每一个分区上都运行此查询。

```kotlin
val start = System.currentTimeMillis()
val deferred = (1..12).map {month ->
  GlobalScope.async {

    val sql = "SELECT passenger_count, " +
        "MAX(CAST(fare_amount AS double)) AS max_fare " +
        "FROM tripdata " +
        "GROUP BY passenger_count"

    val start = System.currentTimeMillis()
    val result = executeQuery(path, month, sql)
    val duration = System.currentTimeMillis() - start
    println("Query against month $month took $duration ms")
    result
  }
}
val results: List<RecordBatch> = runBlocking {
  deferred.flatMap { it.await() }
}
val duration = System.currentTimeMillis() - start
println("Collected ${results.size} batches in $duration ms")
```

下面是在一台 24 核心台式机上运行的示例的输出：

```kotlin
Query against month 8 took 17074 ms
Query against month 9 took 18976 ms
Query against month 7 took 20010 ms
Query against month 2 took 21417 ms
Query against month 11 took 21521 ms
Query against month 12 took 22082 ms
Query against month 6 took 23669 ms
Query against month 1 took 23735 ms
Query against month 10 took 23739 ms
Query against month 3 took 24048 ms
Query against month 5 took 24103 ms
Query against month 4 took 25439 ms
Collected 12 batches in 25505 ms
```

如你所见，总持续时间与最慢查询时间大致相同。

尽管我们已经成功地对分区执行了聚合查询，但我们的结果仍是有重复值的数据批列表。例如，很有可能在每个分区内都出现 `passenger_count = 1` 这样的结果。

## Combining Results 合并结果

对于映射和选择运算符组成的简单查询，可以组合并行查询的结果（类似于 SQL 中的 `UNION ALL` 操作），并且不需要进一步的处理。涉及聚合、排序或连接等更复杂的查询将需要在并行查询的结果上运行辅助查询，以组合结果。术语 "map" 和 "reduce" 经常用于解释这两步的过程。"map" 步骤指的是在分区中并行运行一个查询，"reduce" 步骤是指将结果组合到单个结果中。

对于这个特定的示例，现在需要运行一个次要聚合查询，该查询与针对分区执行的聚合查询几乎相同。其中一个区别是，次要查询可能需要应用不同的集合功能。对于聚合函数 `min`、`max` 和 `sum`，与 map 和 reduce 过程中使用的操作相同，以获取各分区的结果。对于 `count` 表达式，我们不需要每个分区单独的计数值，而是希望看到计数的总和。

```kotlin
val sql = "SELECT passenger_count, " +
        "MAX(max_fare) " +
        "FROM tripdata " +
        "GROUP BY passenger_count"

val ctx = ExecutionContext()
ctx.registerDataSource("tripdata", InMemoryDataSource(results.first().schema, results))
val df = ctx.sql(sql)
ctx.execute(df).forEach { println(it) }
```

这最终将产生如下结果：

```SQL
1,671123.14
2,1196.35
3,350.0
4,500.0
5,760.0
6,262.5
7,80.52
8,89.0
9,97.5
0,90000.0
```

## Smarter Partitioning 更加智能的分区

尽管每个文件使用一个线程的策略在本例中运行良好，但它不能作为通用的分区方法。如果数据源有数千个小分区，那么在每个分区上都启动一个线程的效率会很低。更好的方法是由查询规划器来决定如何在指定数量的工作线程（或执行器）之间共享可用数据。

有些文件格式已经具有自然分区方案。例如，Apache Parquet 文件由多个包含批量列数据的 “行组” 组成。查询规划器可以检查可用的 Parquet 文件，构建行组列表，然后安排在固定数量的线程或执行器中读取这些行组。

甚至可以将此计数应用于非结构化文件，例如 CSV 文件，但这并不是一件容易的事。虽然检查文件大小并将文件分成大小相等的块很容易，但是一条记录可能跨越两个块，因此有必要从边界向后或向前读取以找到记录的起点或重点。查找换行符是不够的，因为这些字符通常也出现在记录中，并也用于界定激励。普遍的做法是将 CSV 文件转换为结构化格式，例如在处理管道前期转换为 Parquet 格式，以提高后续处理的效率。

## Partition Keys 分区键

解决此问题的一种方案是将文件放入目录中，并使用键值对组成目录名称来指定内容。

例如我们可以按如下方式组织文件：

```shell
/mnt/nyxtaxi/csv/year=2019/month=1/tripdata.csv
/mnt/nyxtaxi/csv/year=2019/month=2/tripdata.csv
...
/mnt/nyxtaxi/csv/year=2019/month=12/tripdata.csv
```

有了这种结构，查询规划器现在可以实现一种形式的 “谓词下推”，以限制物理查询计划中包含的分区数量。这种方法通常被称为 “partition pruning 分区修剪”。

## Parallel Joins 并行连接

当使用单个线程执行内部连接时，一种简单的方法是将连接的一侧加载到内存中，然后扫描另一侧，对存储在内存中的数据执行查找。如果连接的一侧可以放入内存，那么这种经典的哈希连接算法是行之有效的。

这种连接的并行版本称为分区哈希连接或并行哈希连接。它包括基于连接键对两个输入进行分区，并在每个分区上执行传统的哈希连接。
