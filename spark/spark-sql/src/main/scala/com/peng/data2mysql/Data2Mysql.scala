package com.peng.data2mysql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 通过SparkSql把结果数据写入到mysql表中
  */
object Data2Mysql extends App {
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
    val result: DataFrame = spark.sql("select * from person where age = 1")

    //保存结果数据到mysql表中
    //mode:指定数据的插入模式
    //overwrite: 表示覆盖，如果表不存在，事先帮我们创建
    //append   :表示追加， 如果表不存在，事先帮我们创建
    //ignore   :表示忽略，如果表事先存在，就不进行任何操作
    //error    :如果表事先存在就报错（默认选项）
    val mode: String = "overwrite"
    val target: String = "t1"
    result.write.mode(mode).jdbc(url, target, properties)
    //关闭
    spark.stop()
}
