# Query Optimizations 请求优化器

> 本章讨论的源代码可以在 KQuery 项目的 [optimizer](https://github.com/andygrove/how-query-engines-work/tree/main/jvm/optimizer) 模块中找到。

我们现在有了功能性查询计划，但是我们有赖于终端用户能够以一种有效的方式构造计划。例如，我们希望用户在构建计划的时候能够尽早的使用过滤器，特别是在连接操作之前，因为这样可以限制需要处理的数据量。

是时候来实现一个简单的基于规则的优化器了，它可以重新安排查询计划以提高其效率。

一旦我们在第 11 章中开始支持 SQL，这将变得更加重要，因为 SQL 语言只定义查询应该如何工作，并不总是允许用户去指定运算符和表达式的求值顺序。

## Rule-Based Optimizations 基于规则的优化器

基于规则进行优化是一种简单而实用的方法。尽管基于规则的优化器也可以应用于物理计划，但是这些优化器通常在创建物理计划之前针对逻辑计划执行。

优化器的工作方式是使用访问者模式遍历逻辑计划，创建计划中每个步骤的副本并应用必要的修改。这事一种比在执行计划时视图改变状态要简单得多的涉及，并且倾向于不可变状态的函数式编程风格。

我们将会使用下面这些接口来表示优化器规则。

```kotlin
interface OptimizerRule {
  fun optimize(plan: LogicalPlan) : LogicalPlan
}
```

现在，我们将研究大多数查询引擎实施的一些常见的优化规则。

### Projection Push-Down 映射下推

> 译者注：如根据表达式自动提前构建所需的映射关联表。

映射下推的目标是在从磁盘读取数据之后和其它查询阶段之前尽快过滤掉列数据，以减少各操作之间保存在内存中的数据量（并且在分布式查询的情况下可能通过网络传输）。

为了知道查询中引用了哪些列，我们必须编写递归代码以检查表达式并构建列数据列表。

```kotlin
fun extractColumns(expr: List<LogicalExpr>,
                   input: LogicalPlan,
                   accum: MutableSet<String>) {

  expr.forEach { extractColumns(it, input, accum) }
}

fun extractColumns(expr: LogicalExpr,
                   input: LogicalPlan,
                   accum: MutableSet<String>) {

  when (expr) {
    is ColumnIndex -> accum.add(input.schema().fields[expr.i].name)
    is Column -> accum.add(expr.name)
    is BinaryExpr -> {
       extractColumns(expr.l, input, accum)
       extractColumns(expr.r, input, accum)
    }
    is Alias -> extractColumns(expr.expr, input, accum)
    is CastExpr -> extractColumns(expr.expr, input, accum)
    is LiteralString -> {}
    is LiteralLong -> {}
    is LiteralDouble -> {}
    else -> throw IllegalStateException(
        "extractColumns does not support expression: $expr")
  }
}
```

有了这段实用的代码，我们就可以继续优化器规则了。注意，对于 `Projection` 映射、`Selection` 选择和 `Aggregate` 聚合计划，我们正在构建列名称列表，但是当我们遇到 `Scan` 扫描（它是一个叶节点）的时候，我们将其替换为查询的列名称列表中的版本。

```kotlin
class ProjectionPushDownRule : OptimizerRule {
  override fun optimize(plan: LogicalPlan): LogicalPlan {
    return pushDown(plan, mutableSetOf())
  }

  private fun pushDown(plan: LogicalPlan,
                       columnNames: MutableSet<String>): LogicalPlan {
    return when (plan) {
      is Projection -> {
        extractColumns(plan.expr, columnNames)
        val input = pushDown(plan.input, columnNames)
        Projection(input, plan.expr)
      }
      is Selection -> {
        extractColumns(plan.expr, columnNames)
        val input = pushDown(plan.input, columnNames)
        Selection(input, plan.expr)
      }
      is Aggregate -> {
        extractColumns(plan.groupExpr, columnNames)
        extractColumns(plan.aggregateExpr.map { it.inputExpr() }, columnNames)
        val input = pushDown(plan.input, columnNames)
        Aggregate(input, plan.groupExpr, plan.aggregateExpr)
      }
      is Scan -> Scan(plan.name, plan.dataSource, columnNames.toList().sorted())
      else -> throw new UnsupportedOperationException()
    }
  }
}
```

给定的输入逻辑计划为：

```SQL
Projection: #id, #first_name, #last_name
  Filter: #state = 'CO'
    Scan: employee; projection=None
```

此优化器规则将其转换为如下计划：

```SQL
Projection: #id, #first_name, #last_name
  Filter: #state = 'CO'
    Scan: employee; projection=[first_name, id, last_name, state]
```

### Predicate Push-Down 谓词下推

谓词下推优化的目的是为了在查询中尽早地过滤掉行，以避免冗余处理。考虑下面的例子，它连接一个 employee 表和 dept 表，然后过滤位于 Colorado 的员工。

```SQL
Projection: #dept_name, #first_name, #last_name
  Filter: #state = 'CO'
    Join: #employee.dept_id = #dept.id
      Scan: employee; projection=[first_name, id, last_name, state]
      Scan: dept; projection=[id, dept_name]
```

这样查询虽然能够产生正确的结果，但也因为对所有员工数据进行连接导致冗余的开销，而不仅是那些位于 Colorado 的员工。谓词下推规则将把过滤器下推到连接中，如下面的查询计划所示。

```SQL
Projection: #dept_name, #first_name, #last_name
  Join: #employee.dept_id = #dept.id
    Filter: #state = 'CO'
      Scan: employee; projection=[first_name, id, last_name, state]
    Scan: dept; projection=[id, dept_name]
```

连接现在将只处理员工信息的一个子集，从而达到更好的性能。

### Eliminate Common Subexpressions 消除公共子表达式

给定一个查询，例如 `SELECT sum(price * qty) as total_price, sum(price * qty * tax_rate) as total_tax FROM ...` 我们可以看到表达式 `price * qty` 出现了两次。我们可以选择重写计划来仅计算一次，而不是执行两次计算。

原计划：

```SQL
Projection: sum(#price * #qty), sum(#price * #qty * #tax)
  Scan: sales
```

优化后：

```SQL
Projection: sum(#_price_mult_qty), sum(#_price_mult_qty * #tax)
  Projection: #price * #qty as _price_mult_qty
    Scan: sales
```

### 将相关子查询转换为连接

给定一个查询，例如 `SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`。一个简单的实现是扫描 `foo` 中的所有行，然后对 `foo` 中的每一行在 `bar` 中执行查找。这样非常低效，因此查询引擎通常将相关子查询转换为连接。这也称为子查询去相关。

上述请求可以重写为 `SELECT foo.id FROM foo JOIN bar ON foo.id = bar.id`。

```SQL
Projection: foo.id
  LeftSemi Join: foo.id = bar.id
    TableScan: foo projection=[id]
    TableScan: bar projection=[id]
```

如果将查询修改为使用 `NOT EXISTS` 而不是 `EXISTS`，那么查询计划将使用 `LeftAnti` 而不是 `LeftSemi` 进行连接。

```SQL
Projection: foo.id
  LeftAnti Join: foo.id = bar.id
    TableScan: foo projection=[id]
    TableScan: bar projection=[id]
```

### Cost-Based Optimizations

基于开销的优化是指使用有关基础数据的统计信息来确定执行特定查询的开销，然后通过寻找开销较小的计划来确定最优的执行计划的优化规则。一个很好的实现会根据基础数据表的大小选择要使用的连接算法，或者选择连接表的顺序。

基于开销优化的一个主要缺点是，它们依赖于有关基础数据库的准确性和详细统计信息的可用性。此类统计信息通常包括每列统计信息，例如空值的数量、不同值的数量、最小值和最大值，以及显示列内值分布的直方图。直方图对于能够检测到像 `state = 'CA'` 这样的谓词可能比 `state = 'WY'` 产生更多的数据行（California 加利福尼亚州是美国人口最多的州，有 3900 万居民，而 Wyoming 怀俄明州是人口最少的州，居民不到 100 万）。

当使用诸如 Orc 或 Parquet 这类的文件格式时，可以使用其中一些统计信息，但是通常有必要运行一个进程来构建这些统计信息，并且当数据量达到 TB 级，这种做法弊大于利，特别是对于 `ad-hoc` （临时的、特别的）查询。
