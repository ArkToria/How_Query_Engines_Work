# 选择一个类型系统

> 本章节所讨论的源代码可以在 KQuery 项目的数据类型模块中找到。

构建查询引擎的第一步是选择一个类型系统来表示查询引擎将要处理的不同类型的数据。有种方法是为查询引擎发明一个专有的类型系统。另一种方法是使用查询引擎所要查询的数据源的类型系统。

如果查询引擎将支持多个数据源（通常是这种情况），那么每所支持的数据源和查询引擎的类型系统之间可能需要进行一些转换，因此使用一个兼容并包的类型系统非常重要。

## 基于行式还是列式？

一个重要的考察因素就是查询引擎是逐行处理，还是以列格式表示数据。

如今许多的查询引擎都是基于 [Volcano Query Planner](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf)，其在物理层上的每一步基本都是以行迭代器为规划的。这种模型实现起来很简单，但是在对数十亿数据进行查询的时候，查询每行的开销往往会快速增加。通过对数据进行批次迭代，可以减少这种开销。此外，如果这些批次代表的是列数据而不是行数据，那么就可以使用 "矢量化处理"，利用 SIMD (单指令多数据) 的又是，用一条 CPU 指令处理一列中的多个值。通过利用 GPU 来并行处理跟大量的数据，这个概念还可以更进一步。

## 可交互性

另一个考虑的因素是，我们可能希望用多种编程语言访问我们的查询疫情。查询引擎用户通常使用 Python、R 或 Java 等语言。我们可能还想构建 ODBC 或 JDBC 驱动程序，便于构建和集成。

考虑到这些需求，最好能找到一种行业标准来表示列式数据，并在进程之间高效交换这些数据。

我认为 Apache Arrow 提供了一个理想的基础，这一结论可能不出所料。

## 类型系统

我们将使用 Apache Arrow 作类型系统的基础。下面的 Arrow 用于表示模式、字段和数据类型。

- Schema 模式为数据源或查询结果提供元数据。模式由一个或多个字段组成
- Field 为模式中的字段提供名称和数据类型，并指定是否允许空值
- FieldVector 为字段提供列式数据存储
- ArrowType 表示数据类型

KQuery 引入了一些额外的类和助手，作为 Apache Arrow 类型系统的抽象。

KQuery 为受支持的 Arrow 数据类型提供了可引用的产量。

```kotlin
object ArrowTypes {
    val BooleanType = ArrowType.Bool()
    val Int8Type = ArrowType.Int(8, true)
    val Int16Type = ArrowType.Int(16, true)
    val Int32Type = ArrowType.Int(32, true)
    val Int64Type = ArrowType.Int(64, true)
    val UInt8Type = ArrowType.Int(8, false)
    val UInt16Type = ArrowType.Int(16, false)
    val UInt32Type = ArrowType.Int(32, false)
    val UInt64Type = ArrowType.Int(64, false)
    val FloatType = ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    val DoubleType = ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    val StringType = ArrowType.Utf8()
}
```

KQuery 并没有直接使用 `FieldVector`，而是引入了一个 `ColumnVector` 接口作为抽象，以提供更方便的访问方法，从而避免了每注数据类型都使用特定的 `FieldVector` 实现。

```kotlin
interface ColumnVector {
  fun getType(): ArrowType
  fun getValue(i: Int) : Any?
  fun size(): Int
}
```

这种抽象也使得标量值的实现称为可能，从而避免了用字面值为列中每个索引重复创建和填入 `FieldVector`。

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

KQuery 还提供了一个 RecordBatch 来表示批量处理的列式数据。

```kotlin
class RecordBatch(val schema: Schema, val fields: List<ColumnVector>) {

  fun rowCount() = fields.first().size()

  fun columnCount() = fields.size

  /** Access one column by index */
  fun field(i: Int): ColumnVector {
      return fields[i]
  }
}
```