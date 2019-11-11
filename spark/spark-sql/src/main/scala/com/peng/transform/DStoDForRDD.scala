package com.peng.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object DStoDForRDD extends App {
    //创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DStoDForRDD").setMaster("local[2]")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //隐式转换
    import spark.implicits._
    //构建数据源生成DataFrame
    //json格式如下,一个json对象一行
    //{"name": "Michael","salary": 3000,"age": 34}
    //{"name": "Andy", "salary": 4500, "age": 23}
    //{"name": "Justin", "salary": 3500, "age": 55}
    val jsonDS: Dataset[Person] = spark.read.json("spark-sql/testdata/rawdata/saveResult/data.json").as[Person]
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
