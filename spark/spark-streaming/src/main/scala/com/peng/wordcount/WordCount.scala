package com.peng.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 单词统计
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        //1 创建SparkConf对象
        val conf: SparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
        //2 创建StreamingContext对象
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
        //3 获取数据流
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        //4 数据处理
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
        val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
        //5 数据输出
        wordCounts.print()
        //6 启动任务
        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
