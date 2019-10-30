package com.peng.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 自定义函数
  */
object UDF extends App {
    //1 创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveResult").setMaster("local[2]")
    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //2 构建数据源生成DataFrame
    val jsonDF: DataFrame = spark.read.json("./spark-sql/testdata/rawdata/saveResult/data.json")
    //3 注册成表
    jsonDF.createTempView("udf")
    //4 实现自定义UDF函数
    //小写转大写
    spark.udf.register("low2Up", new UDF1[String, String] {
        override def call(t1: String): String = {
            t1.toUpperCase()
        }
    }, StringType)
    //大写转小写
    spark.udf.register("up2Low", (x: String) => x.toLowerCase())
    //4 把数据文件中的单词统一转换成大小写
    spark.sql("select name from udf").show()
    spark.sql("select low2Up(name) from udf").show()
    spark.sql("select up2Low(name) from udf").show()

    spark.stop()
}
