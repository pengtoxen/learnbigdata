package com.peng.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDriver {
	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
		val sc = new SparkContext(sparkConf)
		sc.setLogLevel("warn")
		val data: RDD[String] = sc.textFile("spark-core/testdata/rawdata/wordcount/data.txt")
		//创建accumulator并初始化为0
		val accumulator = sc.accumulator(0)
		val words: RDD[String] = data.flatMap(x => {
			//有一条数据就增加1
			accumulator.add(1)
			x.split(" ")
		})
		val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
		val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)
		result.foreach(println)
		println("words lines is :" + accumulator.value)
		sc.stop()
	}
}
