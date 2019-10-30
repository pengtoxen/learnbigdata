package com.peng.dataFromMysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SparkSql从mysql读取数据
  */
object DataFromMysql extends App {
  //1 创建SparkConf对象
  val sparkConf: SparkConf = new SparkConf().setAppName("DataFromMysql").setMaster("local[*]")
  //2 创建SparkSession对象
  val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  //3 读取mysql表的数据
  //3.1 指定mysql连接地址
  val url: String = "jdbc:mysql://localhost:3306/bigdata"
  //3.2 指定要加载的表名
  val tableName: String = "person"
  //3.3 配置连接数据库的相关属性
  val properties: Properties = new Properties()
  //用户名
  properties.setProperty("user", "root")
  //密码
  properties.setProperty("password", "root")
  val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, properties)
  //打印schema信息
  mysqlDF.printSchema()
  //展示数据
  mysqlDF.show()
  //把dataFrame注册成表
  mysqlDF.createTempView("person")
  spark.sql("select * from person where age >1").show()
  spark.stop()
}
