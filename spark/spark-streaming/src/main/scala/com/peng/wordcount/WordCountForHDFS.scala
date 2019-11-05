package com.peng.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountForHDFS {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("test")
        conf.setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(1))
        //hadoop fs -put a.txt /hello
        //hadoop fs -put b.txt /hello
        val hdfsDStream = ssc.textFileStream("hdfs://kka/hello/")

        val result = hdfsDStream.flatMap(_.split(","))
            .map((_, 1))
            .reduceByKey(_ + _)

        result.print()
        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
