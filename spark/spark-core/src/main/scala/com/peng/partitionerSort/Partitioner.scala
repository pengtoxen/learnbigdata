package com.peng.partitionerSort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Partitioner extends App {
	//1.构建sparkConf
	val sparkConf: SparkConf = new SparkConf().setAppName("partitioner").setMaster("local[2]")
	//2.构建sparkContext
	val sc = new SparkContext(sparkConf)
	sc.setLogLevel("warn")
	//3.构建数据源
	val data: RDD[Int] = sc.parallelize(List(12, 20, 30, 569, 1, 100, 4555, 222, 12, 34, 123, 45))
	//4.构建kv对
	val tuple: RDD[(Int, Int)] = data.map(x => (x, 1))
	//5.对应上面的rdd数据进行排序
	val sorted: RDD[(Int, Int)] = tuple.sortBy(_._1, ascending = true)
	//6.转换为原来的数据格式
	val result: RDD[Int] = sorted.map(x => x._1)
	result.foreach(println)
	//7.保存结果数据到文件
	result.saveAsTextFile("./spark-core/testdata/output/partitionerSort")
	//8.关闭sparkContext
	sc.stop()
}