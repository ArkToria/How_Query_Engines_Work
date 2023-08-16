# Logical Plans & Expressions 逻辑计划和表达式

> 本章讨论的源代码可以在 KQuery 项目的 [logical-plan](https://github.com/andygrove/how-query-engines-work/blob/main/jvm/logical-plan) 模块中找到。

一个逻辑计划表示具有已知模式的关系（一组元组）。每个逻辑计划可以由零个或者多个逻辑计划作为输入。对于逻辑计划来说，暴露它的子计划是很方便的，这样以访问者模式就可以对计划进行遍历。

> 译者注：此处的 Logical Plans 指的是一种可递归的树结构，其继承自 Query Plans。

```kotlin
interface LogicalPlan {
  fun schema(): Schema
  fun children(): List<LogicalPlan>
}
```

## Printing Logical Plans 打印逻辑计划

能够以人类可读形式打印逻辑计划在调试过程中有着不可或缺的意义。逻辑哦计划通常打印成以子节点索引的层次结构。

我们可以实现一个简单的递归助手函数，用于逻辑计划的格式化输出。

```kotlin
fun format(plan: LogicalPlan, indent: Int = 0): String {
  val b = StringBuilder()
  0.rangeTo(indent).forEach { b.append("\t") }
  b.append(plan.toString()).append("\n")
  plan.children().forEach { b.append(format(it, indent+1)) }
  return b.toString()
}
```

下面是使用该方法进行逻辑计划格式化的结果示例：

```
Projection: #id, #first_name, #last_name, #state, #salary
  Filter: #state = 'CO'
    Scan: employee.csv; projection=None
```

## Serialization 序列化

有时候需要能够序列化查询计划，以便可以轻松实现在进程之间转移。在早期添加序列化是一个好习惯，对不小心引用了无法序列化的数据结构（例如文件句柄或数据库连接）采取预防措施。

一种方法是采用所实现语言的默认机制来序列化数据结构，以符合 JSON 等格式。例如，在 Java 中可以使用 [Jackson](https://github.com/FasterXML/jackson)，而 Kotlin 有 [kotlinx.serialization](https://github.com/Kotlin/kotlinx.serialization) 库，对于 Rust 有 [serde](https://docs.rs/serde/latest/serde/) crate。

另一种选择是使用 Avro、Thrift 或 Protocol Buffers 这类与语言无关的序列化格式，然后编写代码实现在这种格式和特定语言之间的转换。

自从本书出版第一版以来，出现了一个名为 ["substrait"](https://substrait.io/) 的新标准，目的是为关系代数提供跨语言序列化。我对这个项目感到非常兴奋，并预测它将因代表了查询计划和开创许多集成可能性而成为事实标准。例如，可以使用基于 Java 的成熟的计划器，如 [Apache Calcite](https://calcite.apache.org/)，以 Substrait 格式序列化计划，然后在较低级别的语言（如 C++ 或 Rust）实现的查询引擎中执行该计划。更多信息请访问 [https://substrait.io/](https://substrait.io/)。

## Logical Expressions 逻辑表达式

表达式这一概念是查询计划的基本构建块之一，其可以在运行时对数据进行评估。

下面是查询引擎中常见支持的表达式示例：

|表达式|示例|
|:--|:--|
|Literal Value 字面值|"hello", 12.34|
|Column Reference 列引用|user_id, first_name, last_name|
|Math Expression 数学表达式|salary * state_tax|
|Comparison Expression 比较表达式|x >= y|
|Boolean Expression 布尔表达式|birthday = today() AND age >= 21|
|Arrgregate Expression 聚合表达式|MIN(salary), MAX(salary), SUM(salary), AVG(salary), COUNT(*)|
|Scalar Function 标量函数|CONCAT(first_name, " ", last_name)|
|Aliased Expression 别名表达式|salary * 0.02 AS pay_increase|

当然，所有这些表达式都可以组合起来形成具有深度的嵌套表达式树。表达式求值是递归编程的经典案例。

当我们计划进行查询时，我们需要知道一些关于表达式输出的基本元数据。具体来说，我们需要表达式有一个名称，以便其它表达式可以引用它，并且我们需要知道表达式在求值时将产生得值的数据类型，以便我们可以验证查询计划是否有效。例如，如果我们有一个表达式 `a + b`，则只有当 `a` 和 `b` 都为数字类型的情况下该表达式才有效。

还要注意，表达式的数据类型可以依赖于输入数据。例如，列引用将具有它所引用的列的数据类型，但是比较表达式总是返回布尔值。

```kotlin
interface LogicalExpr {
  fun toField(input: LogicalPlan): Field
}
```

### Column Expressions 列式表达式

列式表达式仅表示对指定列的引用。此表达式的元数据是通过查找输入中指定的列，并返回该列的元数据派生的。注意，这里的术语“列”是指输入逻辑计划生成的列，可以表示数据源中的列，也可以表示对其它输入求值表达式的结果。

```kotlin
class Column(val name: String): LogicalExpr {
  override fun toField(input: LogicalPlan): Field {
    return input.schema().fields.find { it.name == name } ?:
      throw SQLException("No column named '$name'")
  }

  override fun toString(): String {
    return "#$name"
  }
}
```

### Literal Expression 字面值表达式

我们需要将字面值转化为表达式的能力，以便我们可以编写诸如 salary * 0.05 这类的表达式。

下面是字面字符串表达式的示例：

```kotlin
class LiteralString(val str: String): LogicalExpr {
  override fun toField(input: LogicalPlan): Field {
    return Field(str, ArrowTypes.StringType)
  }

  override fun toString(): String {
    return "'$str'"
  }
}
```

下面是字面长整型表达式的示例：

```kotlin
class LiteralLong(val n: Long): LogicalExpr {
  override fun toField(input: LogicalPlan): Field {
      return Field(n.toString(), ArrowTypes.Int64Type)
  }

  override fun toString(): String {
      return n.toString()
  }
}
```

### Binary Expressions 二元表达式

二元表达式是仅有两个输入的表达式。我们将实现三种类型的二元表达式：比较表达式、布尔表达式和数学表达式。因为所有这些的字符串表示都是相同的，所以我们可以使用公共基类提供的 toString 方法。变量 'l' 和 'r' 分别代表左右输入。

```kotlin
abstract class BinaryExpr(
    val name: String,
    val op: String,
    val l: LogicalExpr,
    val r: LogicalExpr) : LogicalExpr {
  override fun toString(): String {
    return "$l $op $r"
  }
}
```

比较表达式，例如：= 或 < 比较两个相同类型数据，并返回一个布尔值。我们还需要实现布尔运算符 `AND` 和 `OR`，它们也接收两个参数并产生一个布尔结果，因此我们也可以为它们使用一个公共的基类。

```kotlin
abstract class BooleanBinaryExpr(
    name: String,
    op: String,
    l: LogicalExpr,
    r: LogicalExpr) : BinaryExpr(name, op, l, r) {
  override fun toField(input: LogicalPlan): Field {
      return Field(name, ArrowTypes.BooleanType)
  }
}
```

这个基类提供了一种实现具体比较表达式的简明方法。

### Comparison Expressions 比较表达式

```kotlin
/** Equality (`=`) comparison */
class Eq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("eq", "=", l, r)

/** Inequality (`!=`) comparison */
class Neq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("neq", "!=", l, r)

/** Greater than (`>`) comparison */
class Gt(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("gt", ">", l, r)

/** Greater than or equals (`>=`) comparison */
class GtEq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("gteq", ">=", l, r)

/** Less than (`<`) comparison */
class Lt(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("lt", "<", l, r)

/** Less than or equals (`<=`) comparison */
class LtEq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("lteq", "<=", l, r)
```

### Boolean Expressions 布尔表达式

基类还提供了一种实现具体布尔逻辑表达式的简明方法。

```kotlin
/** Logical AND */
class And(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("and", "AND", l, r)

/** Logical OR */
class Or(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("or", "OR", l, r)
```

### Math Expressions 数学表达式

数学表达式是另一种特异化的二元表达式。数学表达式通常对相同数据类型的值进行操作，并产生相同数据类型的结果。

```kotlin
abstract class MathExpr(
    name: String,
    op: String,
    l: LogicalExpr,
    r: LogicalExpr) : BinaryExpr(name, op, l, r) {
  override fun toField(input: LogicalPlan): Field {
      return Field("mult", l.toField(input).dataType)
  }
}

class Add(l: LogicalExpr, r: LogicalExpr) : MathExpr("add", "+", l, r)
class Subtract(l: LogicalExpr, r: LogicalExpr) : MathExpr("subtract", "-", l, r)
class Multiply(l: LogicalExpr, r: LogicalExpr) : MathExpr("mult", "*", l, r)
class Divide(l: LogicalExpr, r: LogicalExpr) : MathExpr("div", "/", l, r)
class Modulus(l: LogicalExpr, r: LogicalExpr) : MathExpr("mod", "%", l, r)
```

### Aggregate Expressions 聚合表达式

聚合表达式对输入表达式执行诸如 `MIN`、`MAX`、`COUNT`、`SUM` 或 `AVG` 等聚合函数。

```kotlin
abstract class AggregateExpr(
    val name: String,
    val expr: LogicalExpr) : LogicalExpr {

  override fun toField(input: LogicalPlan): Field {
    return Field(name, expr.toField(input).dataType)
  }

  override fun toString(): String {
    return "$name($expr)"
  }
}
```

对于聚合表达式而言，若聚合数据类型与输入类型相同，我们可以简单地扩展这个基类。

```kotlin
class Sum(input: LogicalExpr) : AggregateExpr("SUM", input)
class Min(input: LogicalExpr) : AggregateExpr("MIN", input)
class Max(input: LogicalExpr) : AggregateExpr("MAX", input)
class Avg(input: LogicalExpr) : AggregateExpr("AVG", input)
```

对于聚合表达式而言，若数据类型并不依赖于输入数据类型，我们需要重写 `toField` 方法。例如："COUNT" 聚合表达式总是生成一个整数，而不管被计数的值的数据类型是什么。

```kotlin
class Count(input: LogicalExpr) : AggregateExpr("COUNT", input) {

  override fun toField(input: LogicalPlan): Field {
    return Field("COUNT", ArrowTypes.Int32Type)
  }

  override fun toString(): String {
    return "COUNT($expr)"
  }
}
```

## Logicasl Plans 逻辑计划

有了逻辑表达式，我们现在可以为查询引擎将要支持的逻辑计划实现各种转换。

### Scan 扫描

`Scan` 扫描逻辑计划表示按可选 `projection` 映射从 `DataSource` 数据源中获取数据。`Scan` 是查询引擎中唯一没有其它逻辑计划作为输入的逻辑计划，它是查询树中的叶节点。

```kotlin
class Scan(
    val path: String,
    val dataSource: DataSource,
    val projection: List<String>): LogicalPlan {

  val schema = deriveSchema()

  override fun schema(): Schema {
    return schema
  }

  private fun deriveSchema() : Schema {
    val schema = dataSource.schema()
    if (projection.isEmpty()) {
      return schema
    } else {
      return schema.select(projection)
    }
  }

  override fun children(): List<LogicalPlan> {
    return listOf()
  }

  override fun toString(): String {
    return if (projection.isEmpty()) {
      "Scan: $path; projection=None"
    } else {
      "Scan: $path; projection=$projection"
    }
  }
}
```

### Projection 映射

`Projection` 映射逻辑计划将映射应用于其输入。映射是带输入数据进行求值的表达式的列表。有时，它是一个简单的列的列表，例如，`SELECT a, b, c FROM foo`。但它也可以包含支持的任何其它类型的表达式。一个更为复杂的例子是 `SELECT (CAST(A AS float) * 3.141592) AS my_float FROM foo`。

```kotlin
class Projection(
    val input: LogicalPlan,
    val expr: List<LogicalExpr>): LogicalPlan {
  override fun schema(): Schema {
    return Schema(expr.map { it.toField(input) })
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Projection: ${ expr.map {
        it.toString() }.joinToString(", ")
    }"
  }
}
```

### Selection (also known as Filter) 选择（也称之为过滤器）

`Selection` 选择逻辑计划应用过滤器表达式来确定应该在其输出中选择（包括）哪些行。这是由 SQL 中的 `WHERE` 子句来表示的。一个简单的例子是 `SELECT * FROM foo WHERE A > 5`。过滤器表达式需要求值结果为布尔类型。

```kotlin
class Selection(
    val input: LogicalPlan,
    val expr: Expr): LogicalPlan {

  override fun schema(): Schema {
    // selection does not change the schema of the input
    return input.schema()
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Filter: $expr"
  }
}
```

### Aggregate 聚合

`Aggregate` 聚合逻辑计划比`Projection 映射`、`Selection 选择`或`Scan 扫描`要复杂得多。它对底层数据进行聚合，例如计算最小值、最大值、平均值和数据求和。聚合通常按照其它列（或表达式）进行分组。一个简单的例子：`SELECT job_title, AVG(salary) FROM employee GROUP BY job_title` (按照职位求平均薪水)。

```kotlin
class Aggregate(
    val input: LogicalPlan,
    val groupExpr: List<LogicalExpr>,
    val aggregateExpr: List<AggregateExpr>) : LogicalPlan {

  override fun schema(): Schema {
    return Schema(groupExpr.map { it.toField(input) } +
         		  aggregateExpr.map { it.toField(input) })
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Aggregate: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
  }
}
```

注意，在此实现中，聚合计划的输出使用分组和聚合表达式进行组织。通常需要将聚合逻辑计划封装在映射中，以便按照原始查询中请求的顺序返回列。