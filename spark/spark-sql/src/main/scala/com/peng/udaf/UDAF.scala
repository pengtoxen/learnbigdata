package com.peng.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 自定义聚合函数
  */
object UDAF extends App {
    //1 创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveResult").setMaster("local[2]")
    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //2 构建数据源生成DataFrame
    val jsonDF: DataFrame = spark.read.json("spark-sql/testdata/rawdata/saveResult/data.json")
    //3 注册成表
    jsonDF.createTempView("udaf")
    //4 实现自定义UDAF函数
    // 注册函数,MyAverage是class需要new一个对象,如果是object则不需要
    spark.udf.register("myAverage", new MyAverage)
    //4 求平均年龄
    spark.sql("select myAverage(salary) from udaf").show()

    spark.stop()
}
