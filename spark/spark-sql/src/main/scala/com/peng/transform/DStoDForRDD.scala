package com.peng.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object DStoDForRDD extends App {
    //创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DStoDForRDD").setMaster("local[2]")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //构建数据源生成DataFrame
    val jsonDS: Dataset[Person] = spark.read.json("./spark-sql/testdata/rawdata/saveResult/data.json").as[Person]

    //隐式转换
    import spark.implicits._
    //DS转换成DF
    val jsonDF = jsonDS.toDF()
    //DS转换成RDD
    val jsonRDD = jsonDS.rdd
    jsonDF.foreach(println(_))

    //定义样例类(定义字段名称和类型)
    case class Person(name: String, salary: Long, age: BigInt) extends Serializable
    jsonRDD.foreach(println(_))
    spark.stop()
}
