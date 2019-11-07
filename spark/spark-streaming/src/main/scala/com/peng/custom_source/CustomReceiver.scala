package com.peng.custom_source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 单词计数
 */
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    //设置了日志的级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.receiverStream(new MyCustomReceiver("localhost", 8888))

    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)
  }
}
