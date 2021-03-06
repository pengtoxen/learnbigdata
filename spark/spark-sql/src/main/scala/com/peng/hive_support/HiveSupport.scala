package com.peng.hive_support

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * hive整合
  */
object HiveSupport extends App {
    //1 创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("HiveSupport").setMaster("local[2]")
    //2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //3 直接使用sparkSession去操作HiveSql语句
    //3.1 创建一张hive表
    spark.sql("create table people(name string,salary double,age int) row format delimited fields terminated by ','")
    //3.2 加载数据到hive表
    spark.sql("load data local inpath 'spark-sql/testdata/rawdata/saveResult/data.text' into table people")
    //3.3 查询
    /**
      * 输出结果
      * +-------+------+---+
      * |   name|salary|age|
      * +-------+------+---+
      * |Michael|3000.0| 34|
      * |   Andy|4500.0| 23|
      * | Justin|3500.0| 55|
      * +-------+------+---+
      */
    spark.sql("select * from people").show()
    spark.stop()
}
