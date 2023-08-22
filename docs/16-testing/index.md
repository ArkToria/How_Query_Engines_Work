# Testing 测试

查询引擎非常复杂，很容易在无意中引入细微的错误，从而导致查询返回不正确的结果，因此进行严格的测试非常重要。

## Unit Testing 单元测试

最好是在第一步是为单个运算符和表达式编写单元测试，用断言来限定给定的输入产生正确的输出。当然错误情况的处理也很重要。

以下是一些在编写单元测试时需要考虑的建议：

- 如果使用了意外的数据类型会发生什么？例如，对于字符串输入进行 `SUM` 求和
- 测试应该涵盖极端情况，例如对数字数据类型使用最小值和最大值，对浮点类型使用 Nan (不是数字)，以确保它们能够被正确处理
- 也应该针对上下溢出的情况进行测试。例如，当两个 `long` (64位) 整数类型相乘时会发生什么？
- 测试还应该确保 null 能够被正确处理

在编写这些测试时，重要的是能够使用任意数据构造记录批和列向量，以用作运算符和表达式的输入。下面是这种实用方法的一个示例：

```kotlin
private fun createRecordBatch(schema: Schema,
                              columns: List<List<Any?>>): RecordBatch {

    val rowCount = columns[0].size
    val root = VectorSchemaRoot.create(schema.toArrow(),
                                       RootAllocator(Long.MAX_VALUE))
    root.allocateNew()
    (0 until rowCount).forEach { row ->
        (0 until columns.size).forEach { col ->
            val v = root.getVector(col)
            val value = columns[col][row]
            when (v) {
                is Float4Vector -> v.set(row, value as Float)
                is Float8Vector -> v.set(row, value as Double)
                ...
            }
        }
    }
    root.rowCount = rowCount

    return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
}
```

下面是针对包含双精度浮点值的两列记录批，计算 “大于等于” (`>=`) 表达式的单元测试示例。

```kotlin
@Test
fun `gteq doubles`() {

    val schema = Schema(listOf(
            Field("a", ArrowTypes.DoubleType),
            Field("b", ArrowTypes.DoubleType)
    ))

    val a: List<Double> = listOf(0.0, 1.0,
                                 Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN)
    val b = a.reversed()

    val batch = createRecordBatch(schema, listOf(a,b))

    val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
    val result = expr.evaluate(batch)

    assertEquals(a.size, result.size())
    (0 until result.size()).forEach {
        assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
    }
}
```

## Integration Testing

单元测试就绪后，下一步是编写集成测试，执行多个运算符和表达式组成的查询，并断言它们按预期产生输出。

有几种流行的方法可以对查询引擎进行集成测试：

- **Imperative Testing 命令式测试**: 硬编码查询和预期结果，要么写成代码，要么存储位包含查询和结果的文件。
- **Comparative Testing 比较测试**: 此方法涉及对另一个（值得信赖的）查询引擎进行查询，并断言两个查询引擎都产生了相同的结果。
- **Fuzzing 模糊**: 生成随机运算符和表达式树来捕获边缘特殊情况，并获得全面测试覆盖率。

## Fuzzing 模糊

由于运算符和表达式树嵌套的特性，运算符和表达式可以无限组合在一起，查询引擎之所以如此复杂很大程度上都来源于这个事实，手工编码测试查询不可能面面俱到。

模糊测试是一种产生随机输入数据的计数。当应用于查询引擎时，这意味着创建随机查询计划。

下面是一个针对 DataFrame 创建随机表达式的示例。这事一种递归方法，可以产生具有深层嵌套结构的表达式树，因此以最大深度构建机制非常重要。

```kotlin
fun createExpression(input: DataFrame, depth: Int, maxDepth: Int): LogicalExpr {
    return if (depth == maxDepth) {
        // return a leaf node
        when (rand.nextInt(4)) {
            0 -> ColumnIndex(rand.nextInt(input.schema().fields.size))
            1 -> LiteralDouble(rand.nextDouble())
            2 -> LiteralLong(rand.nextLong())
            3 -> LiteralString(randomString(rand.nextInt(64)))
            else -> throw IllegalStateException()
        }
    } else {
        // binary expressions
        val l = createExpression(input, depth+1, maxDepth)
        val r = createExpression(input, depth+1, maxDepth)
        return when (rand.nextInt(8)) {
            0 -> Eq(l, r)
            1 -> Neq(l, r)
            2 -> Lt(l, r)
            3 -> LtEq(l, r)
            4 -> Gt(l, r)
            5 -> GtEq(l, r)
            6 -> And(l, r)
            7 -> Or(l, r)
            else -> throw IllegalStateException()
        }
    }
}
```

下面是使用此方法生成表达式的示例。请注意，列应用在这里用哈希之后的索引表示，例如 `#1` 表示索引为 1 的列。几乎可以肯定，此表达式无效（取决于查询引擎的实现），并且在使用 fuzzer 时可以预期到。但这仍然很有价值，因为它将测试错误条件，否则在手动编写测试的时候不一定能够涵盖这些。

```kotlin
#5 > 0.5459397414890019 < 0.3511239641785846 OR 0.9137719758607572 > -6938650321297559787 < #0 AND #3 < #4 AND 'qn0NN' OR '1gS46UuarGz2CdeYDJDEW3Go6ScMmRhA3NgPJWMpgZCcML1Ped8haRxOkM9F' >= -8765295514236902140 < 4303905842995563233 OR 'IAseGJesQMOI5OG4KrkitichlFduZGtjXoNkVQI0Alaf2ELUTTIci' = 0.857970478666058 >= 0.8618195163699196 <= '9jaFR2kDX88qrKCh2BSArLq517cR8u2' OR 0.28624225053564 <= 0.6363627130199404 > 0.19648131921514966 >= -567468767705106376 <= #0 AND 0.6582592932801918 = 'OtJ0ryPUeSJCcMnaLngBDBfIpJ9SbPb6hC5nWqeAP1rWbozfkPjcKdaelzc' >= #0 >= -2876541212976899342 = #4 >= -3694865812331663204 = 'gWkQLswcU' != #3 > 'XiXzKNrwrWnQmr3JYojCVuncW9YaeFc' >= 0.5123788261193981 >= #2
```

在创建逻辑计划的时候也可以采用类似的方法。

```kotlin
fun createPlan(input: DataFrame,
               depth: Int,
               maxDepth: Int,
               maxExprDepth: Int): DataFrame {

    return if (depth == maxDepth) {
        input
    } else {
        // recursively create an input plan
        val child = createPlan(input, depth+1, maxDepth, maxExprDepth)
        // apply a transformation to the plan
        when (rand.nextInt(2)) {
            0 -> {
                val exprCount = 1.rangeTo(rand.nextInt(1, 5))
                child.project(exprCount.map {
                    createExpression(child, 0, maxExprDepth)
                })
            }
            1 -> child.filter(createExpression(input, 0, maxExprDepth))
            else -> throw IllegalStateException()
        }
    }
}
```

下面是该diamagnetic生成的逻辑查询计划的示例：

```SQL
Filter: 'VejBmVBpYp7gHxHIUB6UcGx' OR 0.7762591612853446
  Filter: 'vHGbOKKqR' <= 0.41876514212913307
    Filter: 0.9835090312561898 <= 3342229749483308391
      Filter: -5182478750208008322 < -8012833501302297790
        Filter: 0.3985688976088563 AND #1
          Filter: #5 OR 'WkaZ54spnoI4MBtFpQaQgk'
            Scan: employee.csv; projection=None
```

这种直接的模糊测试方法大概率将产生无效计划。可以通过添加更多上下文感知来对其加以改进，以减少创建无效逻辑计划和表达式的风险。例如，生成 `AND` 表达式可以生成左右表达式并产生布尔结果。然而，只创建正确的计划是有风险的，因为它可能会限制测试覆盖率。理想情况下，应该可以为 fuzzer 配置生成具有不同特征查询计划的规则。
