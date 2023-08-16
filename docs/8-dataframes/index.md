# Building Logical Plans 构建逻辑计划

> 本章讨论的源代码可以在 KQuery 项目的 [logical-plan](https://github.com/andygrove/how-query-engines-work/blob/main/jvm/logical-plan) 模块中找到。

## Building Logical Plans The Hard Way 以复杂方式构建逻辑计划

既然我们已经为逻辑计划的子集定义了类，那么我们就可以以编程方式区组合它们了。

针对 CSV 文件中包含的列：`id`、`first_name`、`last_name`、`state`、`job_title`、`salary`，这里有一些具体的代码，用于构建查询计划 `SELECT * FROM emplopyee WHERE state = 'co'`。

```kotlin
// create a plan to represent the data source
val csv = CsvDataSource("employee.csv")

// create a plan to represent the scan of the data source (FROM)
val scan = Scan("employee", csv, listOf())

// create a plan to represent the selection (WHERE)
val filterExpr = Eq(Column("state"), LiteralString("CO"))
val selection = Selection(scan, filterExpr)

// create a plan to represent the projection (SELECT)
val projectionList = listOf(Column("id"),
                            Column("first_name"),
                            Column("last_name"),
                            Column("state"),
                            Column("salary"))
val plan = Projection(selection, projectionList)

// print the plan
println(format(plan))
```

将计划打印输出如下：

```
Projection: #id, #first_name, #last_name, #state, #salary
    Filter: #state = 'CO'
        Scan: employee; projection=None
```

同样的代码也可以像这样写得更加简洁：

```kotlin
val plan = Projection(
  Selection(
    Scan("employee", CsvDataSource("employee.csv"), listOf()),
    Eq(Column(3), LiteralString("CO"))
  ),
  listOf(Column("id"),
         Column("first_name"),
         Column("last_name"),
         Column("state"),
         Column("salary"))
)
println(format(plan))
```

虽然这样更加简洁，但也更难解释，所以最好能有一种更优雅的方式来创建逻辑计划。这就是 DataFrame 接口方便的地方。

## Building Logical Plans using DataFrames 使用 DataFrames 接口构建逻辑计划

通过实现 DataFrame 风格的 API 让我们以一种更加用户友好的方式构建逻辑查询计划。DataFrame 只是围绕逻辑查询计划的一个抽象，并具有执行转换和事件的方法。它与 fluent-style 构建器的 API 非常相似。

> 译者注：可参考 [https://en.wikipedia.org/wiki/Fluent_interface](https://en.wikipedia.org/wiki/Fluent_interface)

这里是 DataFrame 接口的最小实现，它允许我们将 proijections 映射和 selections 选择应用于一个现有的 DataFrame。

```kotlin
interface DataFrame {
  /** Apply a projection */
  fun project(expr: List<LogicalExpr>): DataFrame

  /** Apply a filter */
  fun filter(expr: LogicalExpr): DataFrame

  /** Aggregate */
  fun aggregate(groupBy: List<LogicalExpr>,
                aggregateExpr: List<AggregateExpr>): DataFrame

  /** Returns the schema of the data that will be produced by this DataFrame. */
  fun schema(): Schema

  /** Get the logical plan */
  fun logicalPlan() : LogicalPlan
}
```

如下是该接口的实现：

```kotlin
class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {
  override fun project(expr: List<LogicalExpr>): DataFrame {
    return DataFrameImpl(Projection(plan, expr))
  }

  override fun filter(expr: LogicalExpr): DataFrame {
    return DataFrameImpl(Selection(plan, expr))
  }

  override fun aggregate(groupBy: List<LogicalExpr>,
                         aggregateExpr: List<AggregateExpr>): DataFrame {
    return DataFrameImpl(Aggregate(plan, groupBy, aggregateExpr))
  }

  override fun schema(): Schema {
    return plan.schema()
  }

  override fun logicalPlan(): LogicalPlan {
    return plan
  }
}
```

在我们应用映射或选择之前，我们需要一种方式来表示底层数据源的初始 DataFrame。这通常通过执行上下文来获得。

> 译者注：即如何从数据源中获取基础数据

如下是执行上下文的一个基础实现，稍后我们将对其进行增强扩展。

```kotlin
class ExecutionContext {
  fun csv(filename: String): DataFrame {
    return DataFrameImpl(Scan(filename, CsvDataSource(filename), listOf()))
  }

  fun parquet(filename: String): DataFrame {
    return DataFrameImpl(Scan(filename, ParquetDataSource(filename), listOf()))
  }
}
```

有了这项基础工作，我们现在可以使用上下文和 DataFrame API 创建一个逻辑查询计划。

```kotlin
val ctx = ExecutionContext()

val plan = ctx.csv("employee.csv")
              .filter(Eq(Column("state"), LiteralString("CO")))
              .select(listOf(Column("id"),
                             Column("first_name"),
                             Column("last_name"),
                             Column("state"),
                             Column("salary")))
```

这样会更加清晰和只管，但是我们还可以再进一步添加一些简便的方法，使之更易于理解。这是 Kotlin 特有的，不过其它语言也有类似的概念。

```kotlin
infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr { return Eq(this, rhs) }
infix fun LogicalExpr.neq(rhs: LogicalExpr): LogicalExpr { return Neq(this, rhs) }
infix fun LogicalExpr.gt(rhs: LogicalExpr): LogicalExpr { return Gt(this, rhs) }
infix fun LogicalExpr.gteq(rhs: LogicalExpr): LogicalExpr { return GtEq(this, rhs) }
infix fun LogicalExpr.lt(rhs: LogicalExpr): LogicalExpr { return Lt(this, rhs) }
infix fun LogicalExpr.lteq(rhs: LogicalExpr): LogicalExpr { return LtEq(this, rhs) }
```

有了这些简便的方法，我们现在可以编写表达式代码来构建逻辑查询计划。

```kotlin
val df = ctx.csv(employeeCsv)
   .filter(col("state") eq lit("CO"))
   .select(listOf(
       col("id"),
       col("first_name"),
       col("last_name"),
       col("salary"),
       (col("salary") mult lit(0.1)) alias "bonus"))
   .filter(col("bonus") gt lit(1000))
```