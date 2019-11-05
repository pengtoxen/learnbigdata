package com.peng.data2mysql

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 通过SparkSql把结果数据写入到mysql表中
  * 采用foreachPartition优化之前的逻辑
  */
object Data2Mysql3 extends App {
  //1 创建SparkSession
  val spark: SparkSession = SparkSession.builder().appName("Data2Mysql").master("local[*]").getOrCreate()
  //2 读取mysql表中数据
  //2.1 定义url连接
  val url: String = "jdbc:mysql://localhost:3306/bigdata"
  //2.2 定义表名
  val table: String = "person"
  //2.3 定义属性
  val properties: Properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "root")
  val mysqlDF: DataFrame = spark.read.jdbc(url, table, properties)
  //把dataFrame注册成一张表
  mysqlDF.createTempView("person")
  //通过SparkSession调用sql方法
  val result: DataFrame = spark.sql("select name,age from person where age = 1")
  //使用foreachPartition的方式,优化后可减少数据库连接
  //每个数据库连接在分区内只连接一次
  result.foreachPartition(iter => {
    val sql: String = "insert into t1(name,age) values(?,?)"
    val url: String = "jdbc:mysql://localhost:3306/bigdata"
    val connection: Connection = DriverManager.getConnection(url, properties)
    val ps: PreparedStatement = connection.prepareStatement(sql)

    iter.foreach(row => {
      val name: String = row.getString(0)
      val age = row.getInt(1)
      ps.setString(1, name)
      ps.setInt(2, age)
      ps.execute()
    })
  })
  //关闭
  spark.stop()
}
