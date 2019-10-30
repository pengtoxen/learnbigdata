package com.peng.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DFtoDSorRDD extends App {
    //创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DFtoDSorRDD").setMaster("local[2]")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //构建数据源生成DataFrame
    val jsonDF: DataFrame = spark.read.json("./spark-sql/testdata/rawdata/saveResult/data.json")
    //隐式转换
    import spark.implicits._

    //定义样例类(定义字段名称和类型)
    case class Person(name: String, salary: Long, age: BigInt) extends Serializable

    //DF转换成DS
    val jsonDS = jsonDF.as[Person]
    jsonDS.foreach(println(_))

    //DF转换成RDD
    val jsonRDD = jsonDF.rdd
    jsonRDD.foreach(println(_))
    spark.stop()
}
