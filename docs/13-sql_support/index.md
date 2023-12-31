# SQL 支持

> 本章讨论的源代码可以在 KQuery 的 [examples](https://github.com/andygrove/how-query-engines-work/tree/main/jvm/examples) 模块中找到。

除了具有手动编码逻辑计划的能力外，在某些情况下，仅编写 SQL 将更加方便。在本章节中，我们将构建一个可以将 SQL 查询转化为逻辑计划的 SQL 解析器和查询规划器。

## Tokenizer 分词器

第一步是将 SQL 查询字符串转换为表示关键字、文字、标识符和操作符的令牌列表。

下面是所有可能令牌的子集，目前已经够用了。

```kotlin
interface Token
data class IdentifierToken(val s: String) : Token
data class LiteralStringToken(val s: String) : Token
data class LiteralLongToken(val s: String) : Token
data class KeywordToken(val s: String) : Token
data class OperatorToken(val s: String) : Token
```

然后我们需要一个 tokenizer 类。在这里介绍这一点不是特别有趣，完整的代码可以在配套的 [Github 仓库](https://github.com/andygrove/how-query-engines-work/blob/main/jvm/sql/src/main/kotlin/SqlTokenizer.kt)中找到。

```kotlin
class Tokenizer {
  fun tokenize(sql: String): List<Token> {
    // see github repo for code
  }
}
```

给定输入 `SELECT a + b FROM c`，我们期望可以得到以下输出：

```kotlin
listOf(
  KeywordToken("SELECT"),
  IdentifierToken("a"),
  OperatorToken("+"),
  IdentifierToken("b"),
  KeywordToken("FROM"),
  IdentifierToken("c")
)
```

## Pratt Parser

> 译者注：相关阅读可参考 [https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html](https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html)

我们将根据 Vaughan R. Pratt 在 1973 年发表的[自顶向下运算符优先级](http://tdop.github.io/)论文，手动编写一个 SQL 解析器。尽管还有其它方法可以构建 SQL 解析器，比如使用解析器生成器和解析器组合器，但我发现 Pratt 的方法很好，而且生成的代码高效，易于理解和调试。

下面是 Pratt 解析器的基本实现。在我看来，它的美丽在于它的简单。表达式解析是通过一个简单的循环来执行的，该循环解析一个 “prefix 前缀” 表达式，然后是可选的 “infix 中缀” 表达式，并继续执行此操作，直到优先级发生改变，使解析器认识到它已经完成了对表达式的解析。当然 `parsePrefix` 和 `parseInfix` 实现可以递归地回到 `parse` 方法中，这就是它变得非常强大的地方。

```kotlin
interface PrattParser {
  /** Parse an expression */
  fun parse(precedence: Int = 0): SqlExpr? {
    var expr = parsePrefix() ?: return null
    while (precedence < nextPrecedence()) {
      expr = parseInfix(expr, nextPrecedence())
    }
    return expr
  }

  /** Get the precedence of the next token */
  fun nextPrecedence(): Int

  /** Parse the next prefix expression */
  fun parsePrefix(): SqlExpr?

  /** Parse the next infix expression */
  fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr
}
```

这个接口引用了一个新的 SqlExpr 类，它将作为解析表达式的表示形式，并且在很大程度上将是逻辑计划中定义的表达式的一对一映射。但是对于二元表达式，我们可以使用其中运算符是字符串这种更为通用的结构，而不是为我们将支持的所有不同的二进制表达式创建单独的数据结构。

下面是 SqlExpr 实现的一些示例：

```kotlin
/** SQL Expression */
interface SqlExpr

/** Simple SQL identifier such as a table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
  override fun toString() = id
}

/** Binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
  override fun toString(): String = "$l $op $r"
}

/** SQL literal string */
data class SqlString(val value: String) : SqlExpr {
  override fun toString() = "'$value'"
}
```

有了这些类，就可以用下面的代码表示表达式 `foo = bar`。

```kotlin
val sqlExpr = SqlBinaryExpr(SqlIdentifier("foo"), "=", SqlString("bar"))
```

## Parsing SQL Expressions 解析 SQL 表达式

让我们通过这种方式来解析一个简单的数学表达式，例如 `1 + 2 * 3`。该表达式由以下标记组成。

```kotlin
listOf(
  LiteralLongToken("1"),
  OperatorToken("+"),
  LiteralLongToken("2"),
  OperatorToken("*"),
  LiteralLongToken("3")
)
```

我们需要创建 `PrattParser` 的 trait 特性实现，然后将令牌传递给构造函数。令牌封装在 `TokenStream` 类中，该类提供了一些方便的方法，例如用于消费下一个令牌的 `next`，以及当我们希望在不消费令牌的情况下查看时的 `peek`。

```kotlin
class SqlParser(val tokens: TokenStream) : PrattParser {
}
```

实现 `nextPrecedence` 方法很简单，因为这里只有少量具任何优先级的令牌，并且我们需要使乘法和除法运算符具有比加减法运算符更高的优先级。注意，这个方法返回的具体数字并不重要，因为它们只是用于比较。在 [PostgreSQL 文档](https://www.postgresql.org/docs/7.2/sql-precedence.html) 中可以找到一个很好的运算符优先级参考。

```kotlin
override fun nextPrecedence(): Int {
  val token = tokens.peek()
  return when (token) {
    is OperatorToken -> {
      when (token.s) {
        "+", "-" -> 50
        "*", "/" -> 60
        else -> 0
      }
    }
    else -> 0
  }
}
```

前缀解析器只需要知道如何解析字面数值。

```kotlin
override fun parsePrefix(): SqlExpr? {
  val token = tokens.next() ?: return null
  return when (token) {
    is LiteralLongToken -> SqlLong(token.s.toLong())
    else -> throw IllegalStateException("Unexpected token $token")
  }
}
```

中缀解析器只需要知道如何解析运算符。注意，在解析运算符之后，此方法将递归地回调倒顶层解析方法，以解析运算符后面的表达式（二元表达式的右侧）。

```kotlin
override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
  val token = tokens.peek()
  return when (token) {
    is OperatorToken -> {
      tokens.next()
      SqlBinaryExpr(left, token.s, parse(precedence) ?:
                    throw SQLException("Error parsing infix"))
    }
    else -> throw IllegalStateException("Unexpected infix token $token")
  }
}
```

优先级逻辑可以通过解析数学表达式 `1 + 2 * 3` 和 `1 * 2 + 3`来证明，它们应该分别被解析为 `1 + (2 * 3)` 和 `(1 * 2) + 3`。

例如：解析 `1 + 2 _ 3 *`。

以下是令牌及其优先级值。

```kotlin
Tokens:      [1]  [+]  [2]  [*]  [3]
Precedence:  [0] [50]  [0] [60]  [0]
```

最终结果将表达式正确地表述为 `1 + (2 * 3)`。

```kotlin
SqlBinaryExpr(
    SqlLong(1),
    "+",
    SqlBinaryExpr(SqlLong(2), "*", SqlLong(3))
)
```

例如：解析 `1 _ 2 + 3*`。

```kotlin
Tokens:      [1]  [*]  [2]  [+]  [3]
Precedence:  [0] [60]  [0] [50]  [0]
```

最终结果将表达式正确地表述为 `(1 * 2) + 3`。

```kotlin
SqlBinaryExpr(
    SqlBinaryExpr(SqlLong(1), "*", SqlLong(2)),
    "+",
    SqlLong(3)
)
```

## Parsing a SELECT statement 解析 SELECT 语句

现在我们已经能够解析一些简单地表达式了，下一步是扩展解析器，以支持将 SELECT 语句解析为具体的语法树 (CST)。请注意，对于其它的解析方法，例如使用像 [ANTLR](https://www.antlr.org/) 这样的解析生成器，会有一个称为抽象语法树 (AST) 的中间阶段，然后需要将其转换为具体语法树，但是使用 Pratt 解析器方法，我们可以直接从令牌转换为具体语法树。

下面是一个示例 CST，它可以表示带有映射和选择的简易单表查询。将在后面的章节中对齐进行扩展以支持更复杂的查询。

```kotlin
data class SqlSelect(
    val projection: List<SqlExpr>,
    val selection: SqlExpr,
    val tableName: String) : SqlRelation
```

## SQL Query Planner SQL 查询规划器

SQL 查询规划器将 SQL 查询树转换为逻辑计划。由于 SQL 语言的灵活性，这将比逻辑计划转换为物理计划要困难得多。例如，考虑下面的简单查询：

```kotlin
SELECT id, first_name, last_name, salary/12 AS monthly_salary
FROM employee
WHERE state = 'CO' AND monthly_salary > 1000
```

虽然这对于阅读的人来说很直观，但是查询的选择部分 (`WHERE` 子句) 引用了一个表达式 (`state`)，该表达式不包含在映射的输出中，因此显然需要在映射前应用，当它同时也应用了另一个表达式 (`salary/12 AS monthly_salary`)，该表达式只有在应用映射后才可用。在使用 `GROUP BY`、`HAVING` 和 `ORDER BY` 子句时，我们也会遇到类似的问题。

这个问题有多种解决方案。一种方案是将此查询转换未以下逻辑计划，将表达式分成两个步骤，一个在映射前，另一个在映射后。但是，这样可行仅仅是因为所选的表达式是一个结合性谓词（只有在所有部分都是正确的情况下，表达式是正确的），而对于更复杂的表达式来说，这种方法可能无法使用。如果该表达式变为 `state = 'CO' OR monthly_salary > 1000`，那么我们将无法执行此操作。

```SQL
Filter: #monthly_salary > 1000
  Projection: #id, #first_name, #last_name, #salary/12 AS monthly_salary
    Filter: #state = 'CO'
      Scan: table=employee
```

一种更加简单通用的方法是将所有必须的表达式加到映射中，以便可以在映射后应用选择，然后通过在另一个映射中封装输出来移除所有多余的列。

```kotlin
Projection: #id, #first_name, #last_name, #monthly_salary
  Filter: #state = 'CO' AND #monthly_salary > 1000
    Projection: #id, #first_name, #last_name, #salary/12 AS monthly_salary, #state
      Scan: table=employee
```

值得注意的是，我们将在后面的章节中构建一个 "Predicate Push Down" 查询优化器规则，它能够优化该计划，并将谓词的 `state = 'CO'` 部分推到计划的更下方，使其位于映射之前。

## Translating SQL Expressions 转换 SQL 表达式

将 SQL 表达式转换未逻辑表达式相当简单，如本示例代码所示：

```kotlin
private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
  return when (expr) {
    is SqlIdentifier -> Column(expr.id)
    is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
    is SqlString -> LiteralString(expr.value)
    is SqlLong -> LiteralLong(expr.value)
    is SqlDouble -> LiteralDouble(expr.value)
    is SqlBinaryExpr -> {
      val l = createLogicalExpr(expr.l, input)
      val r = createLogicalExpr(expr.r, input)
      when(expr.op) {
        // comparison operators
        "=" -> Eq(l, r)
        "!=" -> Neq(l, r)
        ">" -> Gt(l, r)
        ">=" -> GtEq(l, r)
        "<" -> Lt(l, r)
        "<=" -> LtEq(l, r)
        // boolean operators
        "AND" -> And(l, r)
        "OR" -> Or(l, r)
        // math operators
        "+" -> Add(l, r)
        "-" -> Subtract(l, r)
        "*" -> Multiply(l, r)
        "/" -> Divide(l, r)
        "%" -> Modulus(l, r)
        else -> throw SQLException("Invalid operator ${expr.op}")
      }
    }

    else -> throw new UnsupportedOperationException()
  }
}
```

## Planning SELECT 规划 SELECT

如果我们只想支持所选列引用也全都存在于映射中，我们也可以使用一些非常简单的逻辑来构建查询计划。

```kotlin
fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

  // get a reference to the data source
  var df = tables[select.tableName] ?:
      throw SQLException("No table named '${select.tableName}'")

  val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

  if (select.selection == null) {
    // apply projection
    return df.select(projectionExpr)
  }

  // apply projection then wrap in a selection (filter)
  return df.select(projectionExpr)
           .filter(createLogicalExpr(select.selection, df))
}
```

然而，由于选择可以映射的输入和输出，因此我们需要创建一个带有中间映射的更复杂的计划。第一步是通过选择过滤器表达式以确定哪些列是被引用到的。为此，我们将使用访问者模式遍历表达式树，并构建一个可变的列名称集合。

下面是我们将用于遍历表达式树的方法：

```kotlin
private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
  when (expr) {
    is Column -> accumulator.add(expr.name)
    is Alias -> visit(expr.expr, accumulator)
    is BinaryExpr -> {
      visit(expr.l, accumulator)
      visit(expr.r, accumulator)
     }
  }
}
```

至此，我们现在可以编写以下代码，将 SELECT 语句转换为有效的逻辑计划。下面的示例代码并不完美，并且在特殊情况下下可能包含一些错误，例如数据源中的列和别名表达式之间存在名称冲突，但是为了保持代码简洁，我们将暂时忽略这一点。

```kotlin
fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

  // get a reference to the data source
  var df = tables[select.tableName] ?:
    throw SQLException("No table named '${select.tableName}'")

  // create the logical expressions for the projection
  val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

  if (select.selection == null) {
    // if there is no selection then we can just return the projection
    return df.select(projectionExpr)
  }

  // create the logical expression to represent the selection
  val filterExpr = createLogicalExpr(select.selection, df)

  // get a list of columns references in the projection expression
  val columnsInProjection = projectionExpr
    .map { it.toField(df.logicalPlan()).name}
    .toSet()

  // get a list of columns referenced in the selection expression
  val columnNames = mutableSetOf<String>()
  visit(filterExpr, columnNames)

  // determine if the selection references any columns not in the projection
  val missing = columnNames - columnsInProjection

  // if the selection only references outputs from the projection we can
  // simply apply the filter expression to the DataFrame representing
  // the projection
  if (missing.size == 0) {
    return df.select(projectionExpr)
             .filter(filterExpr)
  }

  // because the selection references some columns that are not in the
  // projection output we need to create an interim projection that has
  // the additional columns and then we need to remove them after the
  // selection has been applied
  return df.select(projectionExpr + missing.map { Column(it) })
           .filter(filterExpr)
           .select(projectionExpr.map {
              Column(it.toField(df.logicalPlan()).name)
            })
}
```

## Planning for Aggregate Queries 规划聚合查询

如你所见，SQL 查询规划器相对复杂，解析聚合查询的代码则更有甚之。如果你对此有兴趣了解更多，请参阅源代码。
