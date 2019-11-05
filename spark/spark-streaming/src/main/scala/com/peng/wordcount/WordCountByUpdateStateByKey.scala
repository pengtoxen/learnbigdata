package com.peng.wordcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 单词实时的统计
  *
  * socket 里面有源源不断的数据产生，然后SparkStreaming来获取
  * 这个socket里面的数据，对数据进行处理，进行单词计数。
  */
object WordCountByUpdateStateByKey {

    private val update = (values: Seq[Int], state: Option[Int]) => {
        //当前值
        val currentCount = values.sum
        //历史值
        val lastCount = state.getOrElse(0)
        //Scala里面最后一行代码就是返回值
        Some(currentCount + lastCount)
    }

    def main(args: Array[String]): Unit = {

        //设置了日志的级别
        Logger.getLogger("org").setLevel(Level.ERROR)

        //1. 数据的输入
        //步骤一：创建程序入口
        val conf = new SparkConf()

        //driver executor task

        //如果写的是local那么代码的就是1个线程
        //但是这儿至少需要2个线程才能跑起来，因为一个线程要接收数据，一个线程要处理数据。
        //local[*] 你当前的电脑有多少个cpu core * 就代表是几
        conf.setMaster("local[*]")
        conf.setAppName("word count")

        val ssc = new StreamingContext(conf, Seconds(1))
        //这儿要记得设置一个checkpoint目录
        //建议这儿目录设置为HDFS的目录
        ssc.checkpoint("/Users/test/checkpoint")

        //步骤二：获取数据
        //DStream -> RDD RDD RDD RDD -> 数据流
        val dataDStream = ssc.socketTextStream("localhost", 8888, StorageLevel.MEMORY_AND_DISK_SER)
        //2. 数据的处理
        //步骤三 对数据进行处理
        val wordDStream = dataDStream.flatMap(_.split(","))
        val wordAndOneDStream = wordDStream.map((_, 1))

        /**
          * hadoop,hadoop,hadoop,hive
          *
          * hadoop,1
          * hadoop,1
          * hadoop,1
          * hive,1
          *
          * 会根据key进行分组
          * hadoop,{1,1,1}  => hadoop,3
          * hive,{1}
          *
          * hadoop,hive
          * hadoop,1
          * hive,1
          *
          * hadoop,{1}
          * hive,{1}
          *
          * Option:
          * NONE
          * 没有值
          * Some
          * 有值
          */

        //    wordAndOneDStream.updateStateByKey( (values:Seq[Int],state:Option[Int]) => {
        //      val currentCount = values.sum
        //      val lastCount = state.getOrElse(0)
        //      //Scala里面最后一行代码就是返回值
        //     Some(currentCount+lastCount)
        //    })

        val result = wordAndOneDStream.updateStateByKey(update)

        //3. 数据的输出
        //步骤四，数据的输出
        result.print()

        //步骤五：启动程序
        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
