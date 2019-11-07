package com.peng.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * 自定义聚合函数
  */
class MyAverage extends UserDefinedAggregateFunction {
    // 聚合函数输入参数的数据类型
    override def inputSchema: StructType = StructType(StructField("salary", LongType) :: Nil)

    // 聚合缓冲区中值的数据类型
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    // 返回值的数据类型
    override def dataType: DataType = DoubleType

    // 对于相同的输入是否一直返回相同的输出
    override def deterministic: Boolean = true

    // 初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 工资的总额
        buffer(0) = 0L
        // 工资的个数
        buffer(1) = 0L
    }

    // 相同Executor间的数据合并
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
    }

    // 不同Executor间的数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = {
        var ret = buffer.getLong(0).toDouble / buffer.getLong(1)
        // 保留两位小数
        ret.formatted("%.2f").toDouble
    }
}
