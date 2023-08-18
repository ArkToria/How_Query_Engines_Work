# Physical Plan & Expressions 物理计划和表达式

> 本章讨论的源代码可以在 KQuery 项目的 [physical-plan](https://github.com/andygrove/how-query-engines-work/tree/main/jvm/physical-plan) 模块中找到。
>
> 译者注：此处未找到适合 `Physical` 的表意词，大意指软件逻辑和硬件调度优化设计进行分离。

在第五章中定义的逻辑计划指定了该怎么实现，但是没有阐述如何实现。并且尽管将逻辑和物理计划结合起来可以降低开发复杂度，但将二者分开仍就是一个很好的实践。

将逻辑和物理计划分开的一个原因是，有事执行特定的操作可能有多种方式，这也意味着逻辑计划和物理计划之间存在着一对多的关系。

例如，当进程执行与分布式执行，或者 CPU 执行与 GPU 执行之间可能存在单独的物理计划。

此外，诸如 `Aggregate` 聚合和 `Join` 连接之类的操作可以按性能来权衡各种算法进行实现。当聚合数据已经按照 gouping keys 分组键进行排序的时候，使用 Group Aggregate（也称之为软聚合）是行之有效的，它一次只需要保存一组分组键的状态，并可以在一组分组键结束的时候立即发出结果。而如果是未经排序的数据，则往往使用 Hash 聚合。Hash 聚合通过对键进行分组来维护累加器的 HashMap。

连接的算法实现则更为宽泛，包括嵌套循环连接、排序合并连接和 Hash 连接。

物理计划返回 `RecordBatch` 类型的迭代器。

```kotlin
interface PhysicalPlan {
  fun schema(): Schema
  fun execute(): Sequence<RecordBatch>
  fun children(): List<PhysicalPlan>
}
```

## Physical Expressions 物理表达式

我们已经定义了在逻辑计划中应用的逻辑表达式，但是我们现在需要实现包含代码的物理表达式类，以计算运行时的表达式。

每个逻辑表达式可以由多个物理表达式实现。例如，对将两个数字相加的逻辑表达式 `AddExpr`，我们可以分别由 CPU 和 GPU 进行实现。查询规划器可以根据运行代码所在服务器的硬件能力选择使用哪个实现。

物理表达式针对 `RecordBatch` 进行求值，并且求值结果为 `ColumnVector`。

这是我们将用来表示物理表达式的接口：

```kotlin
interface Expression {
  fun evaluate(input: RecordBatch): ColumnVector
}
```

### Column Expressions 列表达式

列表达式只需要对正在处理的 `RecordBatch` 进行简单的计算并返回 `ColumnVector` 引用。逻辑表达式中按名称引用 `Column`，这对于编写查询语句的用户来说是非常友好的，但是对于物理表达式而言，我们希望避免每次计算表达式都要花费在名称查找上的开销，因此将它改为按序号引用 `(val i: Int)`。

```kotlin
class ColumnExpression(val i: Int) : Expression {

  override fun evaluate(input: RecordBatch): ColumnVector {
    return input.field(i)
  }

  override fun toString(): String {
    return "#$i"
  }
}
```

### Literal Expressions 字面值表达式

物理上实现一个字面值表达式实质上只是简单地将一个字面值包装在类中，并且该类实现了适当的 trait，并为列中每个索引提供相同的值。

> 译者注：Traits 特征，此概念可参考 [Rust 语言](https://doc.rust-lang.org/reference/items/traits.html)，理解为一种抽象接口 interface。

```kotlin
class LiteralValueVector(
    val arrowType: ArrowType,
    val value: Any?,
    val size: Int) : ColumnVector {

  override fun getType(): ArrowType {
    return arrowType
  }

  override fun getValue(i: Int): Any? {
    if (i<0 || i>=size) {
      throw IndexOutOfBoundsException()
    }
    return value
  }

  override fun size(): Int {
    return size
  }
}
```

有了这个类，我们就可以为每种数据类型的字面值表达式创建物理表达式了。

```kotlin
class LiteralLongExpression(val value: Long) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    return LiteralValueVector(ArrowTypes.Int64Type,
                              value,
                              input.rowCount())
  }
}

class LiteralDoubleExpression(val value: Double) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    return LiteralValueVector(ArrowTypes.DoubleType,
                              value,
                              input.rowCount())
  }
}

class LiteralStringExpression(val value: String) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    return LiteralValueVector(ArrowTypes.StringType,
                              value.toByteArray(),
                              input.rowCount())
  }
}
```

### Binary Expressions 二元表达式

对于二元表达式，我们需要计算左右输入表达式，然后根据这些输入计算特定的二元运算符，这样我们就可以提供一个基类来简化每个运算符的实现。

```kotlin
abstract class BinaryExpression(val l: Expression, val r: Expression) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    val ll = l.evaluate(input)
    val rr = r.evaluate(input)
    assert(ll.size() == rr.size())
    if (ll.getType() != rr.getType()) {
      throw IllegalStateException(
          "Binary expression operands do not have the same type: " +
          "${ll.getType()} != ${rr.getType()}")
    }
    return evaluate(ll, rr)
  }

  abstract fun evaluate(l: ColumnVector, r: ColumnVector) : ColumnVector
}
```

### Comparison Expressions 比较表达式

比较表达式只是比较两个输入列中的所有值，并产生一个结果的新列（位向量）。

这里有一个等价运算符的例子：

```kotlin
class EqExpression(l: Expression,
                   r: Expression): BooleanExpression(l,r) {

  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean {
    return when (arrowType) {
      ArrowTypes.Int8Type -> (l as Byte) == (r as Byte)
      ArrowTypes.Int16Type -> (l as Short) == (r as Short)
      ArrowTypes.Int32Type -> (l as Int) == (r as Int)
      ArrowTypes.Int64Type -> (l as Long) == (r as Long)
      ArrowTypes.FloatType -> (l as Float) == (r as Float)
      ArrowTypes.DoubleType -> (l as Double) == (r as Double)
      ArrowTypes.StringType -> toString(l) == toString(r)
      else -> throw IllegalStateException(
          "Unsupported data type in comparison expression: $arrowType")
    }
  }
}
```

### Math Expressions数学表达式

数学表达式的实现与比较表达式的代码非常类似。一个基类可以用于所有数学表达式。

```kotlin
abstract class MathExpression(l: Expression,
                              r: Expression): BinaryExpression(l,r) {

  override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
    val fieldVector = FieldVectorFactory.create(l.getType(), l.size())
    val builder = ArrowVectorBuilder(fieldVector)
    (0 until l.size()).forEach {
      val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
      builder.set(it, value)
    }
    builder.setValueCount(l.size())
    return builder.build()
  }

  abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any?
}
```

下面是扩展此基类的一个特定数学表达式示例：

```kotlin
class AddExpression(l: Expression,
                    r: Expression): MathExpression(l,r) {

  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any? {
      return when (arrowType) {
        ArrowTypes.Int8Type -> (l as Byte) + (r as Byte)
        ArrowTypes.Int16Type -> (l as Short) + (r as Short)
        ArrowTypes.Int32Type -> (l as Int) + (r as Int)
        ArrowTypes.Int64Type -> (l as Long) + (r as Long)
        ArrowTypes.FloatType -> (l as Float) + (r as Float)
        ArrowTypes.DoubleType -> (l as Double) + (r as Double)
        else -> throw IllegalStateException(
            "Unsupported data type in math expression: $arrowType")
      }
  }

  override fun toString(): String {
    return "$l+$r"
  }
}
```

### Aggregate Expressions 聚合表达式

到目前为止，我们看到的表达式都是通过每个 batch 中的一个或多个输入列产生一个输出列。而聚合表达式的情况则更为复杂，因为它们跨多个批量记录数据进行聚合，然后产生一个最终值，因此我们需要引入 accumulator 累加器的概念，并且每个聚合表达式的物理表示需要知道如何为查询引擎传入输入数据的累加器。

下面是表示聚合表达式和累加器的主要接口：

```kotlin
interface AggregateExpression {
  fun inputExpression(): Expression
  fun createAccumulator(): Accumulator
}

interface Accumulator {
  fun accumulate(value: Any?)
  fun finalValue(): Any?
}
```

`Max` 聚合表达式的实现将生成一个特定的 MaxAccumulator。

```kotlin
class MaxExpression(private val expr: Expression) : AggregateExpression {

  override fun inputExpression(): Expression {
    return expr
  }

  override fun createAccumulator(): Accumulator {
    return MaxAccumulator()
  }

  override fun toString(): String {
    return "MAX($expr)"
  }
}
```

下面是 MaxAccumulator 的一个示例实现：

```kotlin
class MaxAccumulator : Accumulator {

  var value: Any? = null

  override fun accumulate(value: Any?) {
    if (value != null) {
      if (this.value == null) {
        this.value = value
      } else {
        val isMax = when (value) {
          is Byte -> value > this.value as Byte
          is Short -> value > this.value as Short
          is Int -> value > this.value as Int
          is Long -> value > this.value as Long
          is Float -> value > this.value as Float
          is Double -> value > this.value as Double
          is String -> value > this.value as String
          else -> throw UnsupportedOperationException(
            "MAX is not implemented for data type: ${value.javaClass.name}")
        }

        if (isMax) {
          this.value = value
        }
      }
    }
  }

  override fun finalValue(): Any? {
    return value
  }
}
```

## Physical Plans 物理计划

有了物理表达式，我们现在可以为查询引擎将要支持的各种转换实现物理计划。

### Scan 扫描

`Scan` 扫描执行计划只需要委托给数据源，并传入映射来限制要加载到内存中的列数量。不执行任何附加逻辑。

```kotlin
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {

  override fun schema(): Schema {
    return ds.schema().select(projection)
  }

  override fun children(): List<PhysicalPlan> {
    // Scan is a leaf node and has no child plans
    return listOf()
  }

  override fun execute(): Sequence<RecordBatch> {
    return ds.scan(projection);
  }

  override fun toString(): String {
    return "ScanExec: schema=${schema()}, projection=$projection"
  }
}
```

### Projection 映射

`Projection` 映射执行计划只需要根据输入的列计算映射表达式，然后生成包含派生列的批量记录。注意，对于按名称引用现有列的映射表达式，派生列只是一个只想输入列的指针或引用，因此不会复制底层数据值。

```kotlin
class ProjectionExec(
    val input: PhysicalPlan,
    val schema: Schema,
    val expr: List<Expression>) : PhysicalPlan {

  override fun schema(): Schema {
    return schema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    return input.execute().map { batch ->
      val columns = expr.map { it.evaluate(batch) }
        RecordBatch(schema, columns)
      }
  }

  override fun toString(): String {
    return "ProjectionExec: $expr"
  }
}
```

### Selection (as known as Filter) 选择（也称之为过滤器）

`Selection` 选择执行计划是第一个重要的计划，因为它有具有条件逻辑，可以确定输入批量记录中的哪些行应该被包含在输出批量数据中。

对于每个输入批量数据，过滤器表达式会计算并返回一个位向量，其中包含表示表达式布尔结果的位，每行对应一位。然后使用该位向量过滤输入列以产生新的输出列。这是一个简单实现，可以针对维向量包含位为全 1 或全 0 的情况进行优化，以避免将无关数据复制到新向量所产生的额外开销。

> 译者注：Apache Arrow 的 Validity Bitmap Buffer 设计思想的简化实现。

```kotlin
class SelectionExec(
    val input: PhysicalPlan,
    val expr: Expression) : PhysicalPlan {
  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    val input = input.execute()
    return input.map { batch ->
      val result = (expr.evaluate(batch) as ArrowFieldVector).field as BitVector
      val schema = batch.schema
      val columnCount = batch.schema.fields.size
      val filteredFields = (0 until columnCount).map {
          filter(batch.field(it), result)
      }
      val fields = filteredFields.map { ArrowFieldVector(it) }
      RecordBatch(schema, fields)
    }

  private fun filter(v: ColumnVector, selection: BitVector) : FieldVector {
    val filteredVector = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
    filteredVector.allocateNew()

    val builder = ArrowVectorBuilder(filteredVector)

    var count = 0
    (0 until selection.valueCount).forEach {
      if (selection.get(it) == 1) {
        builder.set(count, v.getValue(it))
        count++
      }
    }
    filteredVector.valueCount = count
    return filteredVector
  }
}
```

### Hash Aggregate Hash 聚合

`Hash` 聚合计划比之前的计划要复杂得多，因为它必须处理所有输入批量数据，维护累加器的 HashMap，并为正在处理的没一行数据更新累加器。组以后，使用累加器的结果在末尾创建一个批量数据记录，其中包含了聚合查询的结果。

```kotlin
class HashAggregateExec(
    val input: PhysicalPlan,
    val groupExpr: List<Expression>,
    val aggregateExpr: List<AggregateExpression>,
    val schema: Schema
) : PhysicalPlan {
  override fun schema(): Schema {
    return schema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "HashAggregateExec: groupExpr=$groupExpr, aggrExpr=$aggregateExpr"
  }

  override fun execute(): Sequence<RecordBatch> {
    val map = HashMap<List<Any?>, List<Accumulator>>()

    // for each batch from the input executor
    input.execute().iterator().forEach { batch ->

      // evaluate the grouping expressions
      val groupKeys = groupExpr.map { it.evaluate(batch) }

      // evaluate the expressions that are inputs to the aggregate functions
      val aggrInputValues = aggregateExpr.map { 
        it.inputExpression().evaluate(batch) 
      }

      // for each row in the batch
      (0 until batch.rowCount()).forEach { rowIndex ->

        // create the key for the hash map
        val rowKey =
            groupKeys.map {
              val value = it.getValue(rowIndex)
              when (value) {
                is ByteArray -> String(value)
                else -> value
              }
            }

        // println(rowKey)

        // get or create accumulators for this grouping key
        val accumulators = map.getOrPut(rowKey) { aggregateExpr.map { it.createAccumulator() } }

        // perform accumulation
        accumulators.withIndex().forEach { accum ->
          val value = aggrInputValues[accum.index].getValue(rowIndex)
          accum.value.accumulate(value)
        }
      }
    }

    // create result batch containing final aggregate values
    val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
    root.allocateNew()
    root.rowCount = map.size

    val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

    map.entries.withIndex().forEach { entry ->
      val rowIndex = entry.index
      val groupingKey = entry.value.key
      val accumulators = entry.value.value
      groupExpr.indices.forEach { builders[it].set(rowIndex, groupingKey[it]) }
      aggregateExpr.indices.forEach {
        builders[groupExpr.size + it].set(rowIndex, accumulators[it].finalValue())
      }
    }

    val outputBatch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })

    // println("HashAggregateExec output:\n${outputBatch.toCSV()}")
    return listOf(outputBatch).asSequence()
  }
}
```

### Joins 连接

顾名思义，`Join` 运算符用于连接两个相关行。有许多不同类型的连接，伴随着不同的语义。

- `[INNER] JOIN`: 这是最常用的连接类型，它创建了一个包含左右输入行的新关系。如果连接表达式只包含左右输入列之间的等值比较，则这种连接被称为 "equi-join 等值连接"。例如： `SELECT * FROM customer JOIN orders ON customer.id = order.customer_id`。
- `LEFT [OUTER] JOIN`: 左外连接产生的行包含来自左输入的所有值，以及来自右输入的可选行。若右侧没有匹配的结果，则为右侧列生成空值。
- `RIGHT [OUTER] JOIN`: 与左连接操作相反。从右边开始的所有行和左边开始的可用行一起返回。
- `SEMI JOIN`: 半连接类似于左连接，但它只返回左输入中与右输入匹配的行。右输入没有数据返回。并不是所有的 SQL 实现都显式地支持半连接，它们通常被写成子查询语句。例如：`FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`。
- `ANTI JOIN`: 反连接与半连接相反。它只返回与右输入匹配的左输入中的行。例如：`SELECT id FROM foo WHERE NOT EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`。
- `CROSS JOIN`: 交叉连接返回来自左右输入的所有可能的组合行，如果左输入包含 100 行，右输入包含 200 行，则将返回 20,000 行。这被称为笛卡尔积。

KQuery 还没有实现连接操作符。

### Subqueries 子查询

子查询是查询中的查询。它们可以相关也可以非相关（取决于是否涉及到其它关系的连接）。当子查询返回单个值时，它被称为标量子查询。

### Scalar subqueries 标量子查询

标量子查询返回单个值，可以在许多可以使用字面值的 SQL 表达式中使用。

下面是一个相关标量子查询的示例：

```SQL
SELECT id, name, (SELECT count(*) FROM orders WHERE customer_id = customer.id) AS num_orders FROM customers;
```

下面是一个非相关标量子查询的示例：

```SQL
SELECT * FROM orders WHERE total > (SELECT avg(total) FROM sales WHERE customer_state = 'CA');
```

相关子查询在执行之前被转换为连接（这将在第 10 章中解释）。

不相关的查询可以单独执行，结果值可以替换到顶级查询中。

### EXISTS 和 IN 子查询

`EXISTS` 和 `IN` 表达式（以及它们的否定形式 `NOT EXISTS` 和 `NOT IN`）可用于创建半连接和反连接。

如下是一个半连接的示例，它从子查询返回匹配行的左关联 (foo) 中选择所有行。

```SQL
SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id);
```

关联子查询通常在逻辑计划优化期间转换为连接（这将在第 10 章中解释）。

KQuery 也还没有实现子查询。

## Creating Physical Plans 创建物理计划

物理计划就绪后，下一步是构建一个查询规划器，以便从逻辑计划中创建物理计划，这将在下一章中介绍。
