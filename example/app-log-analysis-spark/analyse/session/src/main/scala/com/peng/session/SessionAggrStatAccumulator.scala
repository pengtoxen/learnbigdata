/*
 * Copyright (c) 2018. Peng Inc. All Rights Reserved.
 */

package com.peng.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

    // 保存所有聚合数据
    private val aggrStatMap = mutable.HashMap[String, Int]()

    // 判断是否是初始值
    override def isZero: Boolean = {
        aggrStatMap.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
        val newAcc = new SessionAggrStatAccumulator
        aggrStatMap.synchronized {
            newAcc.aggrStatMap ++= this.aggrStatMap
        }
        newAcc
    }

    // 重置累加器中的值
    override def reset(): Unit = {
        aggrStatMap.clear()
    }

    // 向累加器中添加一个值
    override def add(v: String): Unit = {
        // v不存在新增
        if (!aggrStatMap.contains(v))
            aggrStatMap += (v -> 0)
        // v存在+1
        aggrStatMap.update(v, aggrStatMap(v) + 1)
    }

    // 各个分区的累加器进行合并的方法
    // 合并另一个类型相同的累加器
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
        other match {
            case acc: SessionAggrStatAccumulator => {
                // 初始值是this.aggrStatMap
                // 迭代对象是acc.value
                // 拿初始值和迭代对象的每个元素计算,返回的结果作为下一次计算的初始值
                (this.aggrStatMap /: acc.value) {
                    // map就是初始值
                    // map += ----> 将map的v累加后, 更新
                    case (map, (k, v)) => map += (k -> (v + map.getOrElse(k, 0))) }
            }
        }
    }

    // 获取累加器中的值
    override def value: mutable.HashMap[String, Int] = {
        this.aggrStatMap
    }
}
