# Query Planner 查询规划器

> 本章讨论的源代码可以在 KQuery 项目的 [query-planner](https://github.com/andygrove/how-query-engines-work/tree/main/jvm/query-planner) 模块中找到。

我们已经定义了逻辑和物理查询计划，现在我们需要一个可以将逻辑计划转换为物理计划的查询规划器。

查询规划器可以根据配置选项或者目标平台的硬件功能选择不同的物理计划。例如，查询可以在 CPU 或 GPU 上执行，也可以在单个节点上执行，或者分布在集群中。

## Translating Logical Expressions 翻译逻辑表达式

第一步是定义一个递归地将逻辑表达式翻译成物理表达式的方法。下面地代码示例演示了一个基于 switch 语句的实现，并展示了如何翻译一个二元表达式（它有两个输入表达式），从而使代码递归回到同一个方法来翻译这些输入。这种方法遍历整个逻辑表达式树并创建相应的物理表达式树。

```kotlin
fun createPhysicalExpr(expr: LogicalExpr,
                       input: LogicalPlan): PhysicalExpr = when (expr) {
  is ColumnIndex -> ColumnExpression(expr.i)
  is LiteralString -> LiteralStringExpression(expr.str)
  is BinaryExpr -> {
    val l = createPhysicalExpr(expr.l, input)
    val r = createPhysicalExpr(expr.r, input)
    ...
  }
  ...
}
```

下面几节将解释每种类型表达式的实现。

### Column Expressions 列表达式

逻辑列表达式按名称引用列，但物理表达式使用列索引以提高性能，因此在查询规划器需要执行从列名到列索引的转换，并在列名无效时抛出异常。

下面这个简化的示例按第一个列名进行查找，而并没有监测是否有多个匹配列，这应该是个错误的条件。

```kotlin
is Column -> {
  val i = input.schema().fields.indexOfFirst { it.name == expr.name }
  if (i == -1) {
    throw SQLException("No column named '${expr.name}'")
  }
  ColumnExpression(i)
```

### Literal Expressions 字面表达式

字面值的物理表达式是非常直观的，并且从逻辑到物理表达式的映射也很简单，因为我们只需要拷贝字面值即可。

```kotlin
is LiteralLong -> LiteralLongExpression(expr.n)
is LiteralDouble -> LiteralDoubleExpression(expr.n)
is LiteralString -> LiteralStringExpression(expr.str)
```

### Binary Expressions 二元表达式

要为二元表达式创建物理表达式，我们首先需要为左右输入创建物理表达式，然后我们再创建特定的物理表达式。

```kotlin
is BinaryExpr -> {
  val l = createPhysicalExpr(expr.l, input)
  val r = createPhysicalExpr(expr.r, input)
  when (expr) {
    // comparision
    is Eq -> EqExpression(l, r)
    is Neq -> NeqExpression(l, r)
    is Gt -> GtExpression(l, r)
    is GtEq -> GtEqExpression(l, r)
    is Lt -> LtExpression(l, r)
    is LtEq -> LtEqExpression(l, r)

    // boolean
    is And -> AndExpression(l, r)
    is Or -> OrExpression(l, r)

    // math
    is Add -> AddExpression(l, r)
    is Subtract -> SubtractExpression(l, r)
    is Multiply -> MultiplyExpression(l, r)
    is Divide -> DivideExpression(l, r)

    else -> throw IllegalStateException(
        "Unsupported binary expression: $expr")
    }
}
```

## Translating Logical Plans 翻译逻辑计划

我们需要实现一个递归方法以遍历逻辑计划树，并将其转换为物理计划树，使用与去前面转换表达式相同的模式。

```kotlin
fun createPhysicalPlan(plan: LogicalPlan) : PhysicalPlan {
  return when (plan) {
    is Scan -> ...
    is Selection -> ...
    ...
}
```

### Scan 扫描

翻译 `Scan` 扫描计划只需要复制数据源引用和逻辑计划的映射。

```kotlin
is Scan -> ScanExec(plan.dataSource, plan.projection)
```

### Projection 映射

翻译一个映射有两步。首先，我们需要为映射的输入创建一个物理计划，然后我们需要将映射的逻辑表达式转换为物理表达式。

```kotlin
is Projection -> {
  val input = createPhysicalPlan(plan.input)
  val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
  val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
  ProjectionExec(input, projectionSchema, projectionExpr)
}
```

### Selection (also known as Filter) 选择（也称之为过滤器）

查询计划中 `Selection` 选择的翻译步骤与 `Projection` 映射非常相似。

```kotlin
is Selection -> {
  val input = createPhysicalPlan(plan.input)
  val filterExpr = createPhysicalExpr(plan.expr, plan.input)
  SelectionExec(input, filterExpr)
}
```

### Aggregate 聚合

聚合查询的查询规划步骤包括计算可选分组键的表达式和聚合函数的输入表达式，然后创建物理聚合表达式。

```kotlin
is Aggregate -> {
  val input = createPhysicalPlan(plan.input)
  val groupExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
  val aggregateExpr = plan.aggregateExpr.map {
    when (it) {
      is Max -> MaxExpression(createPhysicalExpr(it.expr, plan.input))
      is Min -> MinExpression(createPhysicalExpr(it.expr, plan.input))
      is Sum -> SumExpression(createPhysicalExpr(it.expr, plan.input))
      else -> throw java.lang.IllegalStateException(
          "Unsupported aggregate function: $it")
    }
  }
  HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
}
```
