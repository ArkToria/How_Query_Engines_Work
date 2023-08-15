# 什么是查询引擎

查询引擎是一种软件，它可以对数据进行查询，以产生问题的答案，例如：

- 今年到目前为止，我每月的平均销售额是多少？
- 在过去的一天里，我的网站上最受欢迎的五个网页是什么？
- 与一年前相比，网络流量的月度情况如何？

最广泛使用的查询语言是结构化查询语言 (简称 SQL)。许多开发人员在他们的职业生涯中都会遇到关系型数据库，如 MySQL、Postgres、Oracle 或 SQL Server。所有这些数据库都包含支持 SQL 的查询引擎。

这里有一些 SQL 请求示例：

> SQL 示例：月平均销售额

```sql
SELECT month, AVG(sales)
FROM product_sales
WHERE year = 2020
GROUP BY month;
```

> SQL 示例：昨天最热门的五个网页

```sql
SELECT page_url, COUNT(*) AS num_visits
FROM apache_log
WHERE event_date = yesterday()
GROUP BY page_url
ORDER BY num_visits DESC
LIMIT 5;
```

SQL 功能强大且广为人知，但在所谓的 “大数据” 世界中存在局限性，数据科学家通常需要将自定义代码和查询混合在一起。Apache Hadoop、Apache Hive 和 Apache Spark 等平台和工具现在被广泛用于查询和操作大量数据。

> Apache Spark 使用 DataFrame 查询示例：

```kotlin
val spark: SparkSession = SparkSession.builder
  .appName("Example")
  .master("local[*]")
  .getOrCreate()

val df = spark.read.parquet("/mnt/nyctaxi/parquet")
  .groupBy("passenger_count")
  .sum("fare_amount")
  .orderBy("passenger_count")

df.show()
```

## 为什么查询引擎广受欢迎

数据增长的加速度越来越大，通常无法在一台计算机上容纳。需要专业的工程师来编写分布式代码以查询数据，并且每次需要从数据中获取新答案时都编写自定义代码是不切实际的。查询引擎提供了一组标准操作和转换方式，因此终端用户可以通过以不同方式将简单的查询语言或应用程序编程接口 (API) 进行组合和转换，并进行调优以获得良好的性能。

## 本书涵盖了什么内容

本书概述了构建通用查询引擎所涉及的每个步骤。本书中讨论的查询引擎是专门为本书开发的一个简单的查询疫情，其代码是在编写本书内容的同时开发的，以确保我在面临涉及决策时可以编写有关的主题内容。

## 源代码

本书所讨论的完整的查询引擎代码在 Github 仓库：

> https://github.com/andygrove/how-query-engines-work

有关使用 Gradle 构建项目的最新说明请参阅项目中的 README 文档。

## 为什么使用 Kotlin?

本书的重点是查询引擎设计，这通常是编程语言不可或缺的。我在本书中之所以选择 Kotlin，是因为它简洁易懂。并且它也与 Java 100% 兼容，这意味着您可以从 Java 或者其它基于 Java 的语言 (比如 Scala) 调用 Kotlin 代码。

尽管如此，Apache Arrow 项目中的 DataFusion 查询引擎也主要是基于本书中的设计。对 Rust 比 JVM 更感兴趣的读者可以参考 DataFusion 源代码和本书。