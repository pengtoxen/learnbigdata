package com.peng.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义分区
  * 前提条件:产生shuffle
  */
object Partitioner extends App {
    //1.构建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("partitioner").setMaster("local[*]")
    //2.构建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3.构建数据源
    val data = sc.parallelize(List("hadoop", "hdfs", "hive", "spark", "flume", "kafka", "flink", "azkaban"))
    //4.获取每一个元素的长度
    val wordLengthRDD: RDD[(String, Int)] = data.map(x => (x, x.length))
    //5.对应上面的rdd数据进行自定义分区
    val result = wordLengthRDD.partitionBy(new MyPartitioner(4))
    //6.保存结果数据到文件
    result.saveAsTextFile("spark-core/testdata/output/partitioner")
    //7.关闭sparkContext
    sc.stop()
}
