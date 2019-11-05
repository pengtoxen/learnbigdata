package com.peng.saveresult

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveResult extends App {
  //1 创建SparkConf对象
  val sparkConf: SparkConf = new SparkConf().setAppName("SaveResult").setMaster("local[2]")
  //2 创建SparkSession对象
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  //3 加载数据源
  /**
    * 这样的格式
    * {"name":"Michael", "salary":3000}
    * {"name":"Andy", "salary":4500}
    * {"name":"Justin", "salary":3500}
    * {"name":"Berta", "salary":4000}
    */
  val jsonDF: DataFrame = spark.read.json("./spark-sql/testdata/rawdata/saveResult/data.json")
  jsonDF.printSchema()
  jsonDF.show()
  //4 把DataFrame注册成表
  jsonDF.createTempView("data")
  //5 统计分析
  val result: DataFrame = spark.sql("select * from data")

  //保存结果数据到不同的外部存储介质中
  //5.1 保存结果数据到文本文件  ----  保存数据成文本文件目前只支持单个字段，不支持多个字段
  result.select("name").write.text("./spark-sql/testdata/output/saveResult/text")
  //5.2 保存结果数据到json文件
  result.write.json("./spark-sql/testdata/output/saveResult/json")
  //5.3 保存结果数据到parquet文件
  result.write.parquet("./spark-sql/testdata/output/saveResult/parquet")
  //save方法保存结果数据，默认的数据格式就是parquet
  result.write.save("./spark-sql/testdata/output/saveResult/save")
  //5.5 保存结果数据到csv文件
  result.write.csv("./spark-sql/testdata/output/saveResult/csv")
  //5.6 保存结果数据到表中,保存在内存中,可以用来重新操作
  result.write.saveAsTable("t1")
  //5.7  按照单个字段进行分区 分目录进行存储
  result.write.partitionBy("age").json("./spark-sql/testdata/output/saveResult//partitions")
  //5.8  按照多个字段进行分区 分目录进行存储
  result.write.partitionBy("age", "salary").json("./spark-sql/testdata/output/saveResult//numPartitions")
  spark.stop()
}
