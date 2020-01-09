package com.peng.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SparkSQL 与 SparkStreaming整合
  */
object StreamingWithSQL {

	def main(args: Array[String]): Unit = {

		//设置了日志的级别
		Logger.getLogger("org").setLevel(Level.ERROR)
		val conf = new SparkConf()
		conf.setAppName("StreamingWithSQL")
		conf.setMaster("local[2]")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc, Seconds(2))
		val dataStream = ssc.socketTextStream("localhost", 8888)

		val wordDStream = dataStream.flatMap(_.split(","))

		wordDStream.foreachRDD(rdd => {

			//步骤一：导入隐式转换
			val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

			//步骤二：把RDD 转换成为 DataFrame
			//rdd -> DataFrame/Dataset -> SQL
			import spark.implicits._
			val dataFrame = rdd.toDF("word")

			//步骤三：注册成为一张表
			dataFrame.createOrReplaceTempView("words")

			//步骤四：写SQL语句
			//Mysql
			spark.sql("select word,count(*) as wordCount from words group by word").show()
		})

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}
