# Query Execution 查询执行

我们现在可以编写代码来对 CSV 文件执行优化后的查询。

在使用 KQuery 执行查询之前，使用一个可信的替代方案可能很有用，以便我们知道正确的结果应该是什么，并获得一些基线性能指标以进行比较。

## Apache Spark Example Apache Spark 示例

> 本章讨论的源代码可以在 KQuery 的 [spark](https://github.com/andygrove/how-query-engines-work/tree/main/spark) 模块中找到。

首先我们需要创建一个 Spark 上下文。请注意，我们使用单线程执行，以便我们可以相对公平地比较 KQuery 中单线程实现的性能。

```SQL
val spark = SparkSession.builder()
  .master("local[1]")
  .getOrCreate()
```

下一步，我们需要根据上下文将 CSV 文件注册成 DataFrame。

```kotlin
val schema = StructType(Seq(
  StructField("VendorID", DataTypes.IntegerType),
  StructField("tpep_pickup_datetime", DataTypes.TimestampType),
  StructField("tpep_dropoff_datetime", DataTypes.TimestampType),
  StructField("passenger_count", DataTypes.IntegerType),
  StructField("trip_distance", DataTypes.DoubleType),
  StructField("RatecodeID", DataTypes.IntegerType),
  StructField("store_and_fwd_flag", DataTypes.StringType),
  StructField("PULocationID", DataTypes.IntegerType),
  StructField("DOLocationID", DataTypes.IntegerType),
  StructField("payment_type", DataTypes.IntegerType),
  StructField("fare_amount", DataTypes.DoubleType),
  StructField("extra", DataTypes.DoubleType),
  StructField("mta_tax", DataTypes.DoubleType),
  StructField("tip_amount", DataTypes.DoubleType),
  StructField("tolls_amount", DataTypes.DoubleType),
  StructField("improvement_surcharge", DataTypes.DoubleType),
  StructField("total_amount", DataTypes.DoubleType)
))

val tripdata = spark.read.format("csv")
  .option("header", "true")
  .schema(schema)
  .load("/mnt/nyctaxi/csv/yellow_tripdata_2019-01.csv")

tripdata.createOrReplaceTempView("tripdata")
```

最后我们可以继续对 DataFrame 执行 SQL。

```kotlin
val start = System.currentTimeMillis()

val df = spark.sql(
  """SELECT passenger_count, MAX(fare_amount)
    |FROM tripdata
    |GROUP BY passenger_count""".stripMargin)

df.foreach(row => println(row))

val duration = System.currentTimeMillis() - start

println(s"Query took $duration ms")
```

在我的台式机上执行代码输出结果如下：

```SQL
[1,623259.86]
[6,262.5]
[3,350.0]
[5,760.0]
[9,92.0]
[4,500.0]
[8,87.0]
[7,78.0]
[2,492.5]
[0,36090.3]
Query took 14418 ms
```

### KQuery Examples KQuery 示例

> 本章讨论的源代码可以在 KQuery 的 [examples](https://github.com/andygrove/how-query-engines-work/tree/main/jvm/examples) 模块中找到。

下面是用 KQuery 实现的等效查询。请注意，这段代码与 Spark 示例不同，因为 KQuery 还没有指定 CSV 文件模式的选项，所以所有数据类型都是字符串。这意味着我们需要向查询计划添加显式强制转换，以将车费金额列转化为数值类型。

```kotlin
val time = measureTimeMillis {

val ctx = ExecutionContext()

val df = ctx.csv("/mnt/nyctaxi/csv/yellow_tripdata_2019-01.csv", 1*1024)
            .aggregate(
               listOf(col("passenger_count")),
               listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

val optimizedPlan = Optimizer().optimize(df.logicalPlan())
val results = ctx.execute(optimizedPlan)

results.forEach { println(it.toCSV()) }

println("Query took $time ms")
```

这将在我的台式机上产生以下输出：

```SQL
Schema<passenger_count: Utf8, MAX: FloatingPoint(DOUBLE)>
1,623259.86
2,492.5
3,350.0
4,500.0
5,760.0
6,262.5
7,78.0
8,87.0
9,92.0
0,36090.3

Query took 6740 ms
```

我们可以看到，结果与 Apache Spark 生成的结果相匹配。我们还看到其在当前输入规模下的性能优化十分可观。由于 Apache Spark 针对 “大数据” 进行了优化，因此在处理更大的数据集时，它的性能很可能会超过 KQuery。

### Removing The Query Optimizer 移除查询优化器

让我们一处这些优化，看看它们对性能有多大帮助。

```kotlin
val time = measureTimeMillis {

val ctx = ExecutionContext()

val df = ctx.csv("/mnt/nyctaxi/csv/yellow_tripdata_2019-01.csv", 1*1024)
            .aggregate(
               listOf(col("passenger_count")),
               listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

val results = ctx.execute(df.logicalPlan())

results.forEach { println(it.toCSV()) }

println("Query took $time ms")
```

这将在我的台式机上产生如下输出：

```SQL
1,623259.86
2,492.5
3,350.0
4,500.0
5,760.0
6,262.5
7,78.0
8,87.0
9,92.0
0,36090.3

Query took 36090 ms
```

结果是一致的，但执行查询所花费的时间大约多了五倍。这充分显现了上一章中所讨论的映射下推优化所带来的好处。
