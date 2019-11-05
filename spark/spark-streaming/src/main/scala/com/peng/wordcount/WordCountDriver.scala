package com.peng.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountDriver {
    def main(args: Array[String]): Unit = {
        //1 创建SparkConf对象
        //如果写的是local那么代码的就是1个线程
        //但是这儿至少需要2个线程才能跑起来，因为一个线程要接收数据，一个线程要处理数据。
        //local[*] 你当前的电脑有多少个cpu core * 就代表是几
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
        //2 创建StreamingContext对象
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
        //3 获取数据流
        /**
          * SparkCore
          *
          * val sc=new SparkContext(conf)
          * //获取数据
          * val rdd = sc.textFile(xxx)
          *
          * DStream
          *
          * you,jump
          * i,jump
          *
          * you,1
          * jump,2
          * i,1
          *
          */
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        //4 数据处理
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
        val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
        //5 数据输出
        wordCounts.print()
        //6 启动任务
        ssc.start()
        //等待计算结束
        ssc.awaitTermination()
        ssc.stop()
    }
}
