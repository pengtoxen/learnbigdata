package com.peng.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果这个功能要用storm去实现
  * 非常麻烦
  * 每隔4秒统计最近6秒的情况
  */
object Window {
    def main(args: Array[String]): Unit = {
        //设置了日志的级别
        Logger.getLogger("org").setLevel(Level.ERROR)

        val conf = new SparkConf()
        conf.setAppName("TransformTest")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(2))

        val dataStream = ssc.socketTextStream("localhost", 8888)

        val wordAndOneDStream = dataStream.flatMap(_.split(",")).map((_, 1))
        //代表每隔4秒 统计最近6秒的单词
        //6秒就是指window的大小
        //4秒就是指滑动的大小
        //这儿的这两数，必须得是Batch interval的倍数。
        val result = wordAndOneDStream.reduceByKeyAndWindow(
            (x: Int, y: Int) => x + y,
            Seconds(6),
            Seconds(4)
        )
        //类似于mapPartition函数,针对每个rdd操作
        result.foreachRDD(rdd => {
            //rdd
            /**
              * RDD -> mysql
              * RDD -> hbase
              * RDD -> redis
              * RDD -> Kafka
              */
            rdd.foreachPartition(partition => {
                partition.foreach(record => {
                    //针对每条数据怎么处理就是大家的事了。
                })
            })
        })
        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
