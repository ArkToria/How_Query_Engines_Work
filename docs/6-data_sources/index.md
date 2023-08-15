# Data Sources 数据源

> 本章讨论的源代码可以在 KQuery 项目的 datasource 模块中找到。

如果没有可读取的数据源，查询引擎便无计可施，为此我们希望能够支持多种数据源。因此为查询引擎创建一个可以用来与数据源交互的接口就显得尤为重要。这也允许用户将我们的查询引擎用于他们自定义的数据源。数据源通常是文件或者数据库，当也可以是内存对象。

## Data Source Interface 数据源接口

在查询计划过程中，必须要了解数据源的模式 (schema)，这样才能验证查询计划，确保所引用的列存在，并且数据类型与用于引用列的表达式兼容。在某些情况下，可能无法获得模式，因为某些数据源没有固定的模式，这种情况通常被称为 `schema-less`。JSON 文档就是一个无模式数据源的例子。

在执行期间，我们需要能够从数据源中获取数据的能力，并要能够指定将哪些列加载到内存中以提高效率。如果查询不引用列，就没必要将其加载到内存中。

KQuery 数据源接口

```kotlin
interface DataSource {

  /** Return the schema for the underlying data source */
  fun schema(): Schema

  /** Scan the data source, selecting the specified columns */
  fun scan(projection: List<String>): Sequence<RecordBatch>
}
```

## Data Source Examples 数据源示例

### Comma-Separated Values (CSV)

CSV 文件是每行一个记录的文本文件，字段之间使用逗号分隔，因此称为 “逗号分隔值”。CSV 文件不包含模式信息（除文件第一行的可选列名），尽管可以通过想读取文件来派生出模式。但这可能是一个开销高昂的操作。

### [JSON](https://www.json.org/json-en.html)

JavaScript Object Notation 格式 (JSON) 是另一种流行的基于文本的文件格式。与 CSV 文件不同，JSON 的文件是结构化的，可以存储复杂的嵌套数据类型。

### [Parquet](https://parquet.apache.org/)

Parquet 的创建是为了提供一种压缩、高效的列式数据表示，它是 Hadoop 生态系统中一种流行的文件格式。Parquet 从一开始就考虑到了复杂的嵌套数据结构，并使用了 Dremel 论文中描述的 `record shredding and assembly` 算法。

Parquet 文件包含模式信息，数据按批次存储 (称为 “行组”)，其中每个批次由列组成。行组可以包含压缩数据，也可以包含可选的元数据，例如每列的最大值和最小值。可以对查询引擎进行优化，以使用该元数据来确定在扫描期间可以跳过行组的时机。

### [Optimized Row Columnar (Orc)](https://orc.apache.org/docs/)

行优化列 (Orc) 格式类似于 Parquet 格式。数据以列传进行批量储存，称为 “stripes 条纹”。