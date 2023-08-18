# SQL 支持

> 本章讨论的源代码可以在 KQuery 的 [examples](https://github.com/andygrove/how-query-engines-work/tree/main/jvm/examples) 模块中找到。

除了具有手动编码逻辑计划的能力外，在某些情况下，仅编写 SQL 将更加方便。在本章节中，我们将构建一个可以将 SQL 查询转化为逻辑计划的 SQL 解析器和查询计划器。

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
