package com.peng.transform

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 黑名单过滤【transform】
  * 这个是我故意引入进来的这么一个小案例，虽然我用的是单词技术的
  * 方式跟大家去讲解，但是企业里面这种业务场景还挺常见的。
  *
  * 我们一般就是就是单词计数，但是现在我们有个要求，我们要进行
  * 单词黑名单过滤，
  * hadoop,hadoop,spark,!,?,hadoop
  * 我们之前的单词计数，把一些特殊的符号也给计算进去了，
  * 但是实际上我们的业务的要求，这些特殊的符号是不参与计算的。
  * 我们是不是要把这些不参与的这些符号，要当作一个黑名单处理。
  * 然后处理的时候遇到黑名单里面的数据，到时候不进行统计。
  * 以前：
  * Hadoop，3
  * spark，1
  * ！，1
  * ？，1
  *
  */
object TransformFilter {
    def main(args: Array[String]): Unit = {
        //设置了日志的级别
        Logger.getLogger("org").setLevel(Level.ERROR)

        val conf = new SparkConf()
        conf.setAppName("TransformTest")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(2))

        //模拟一个黑名单
        //虽然我这儿模拟的是一个黑名单，但是大家要知道这个黑名单
        //其实在企业项目里面，是从其他的存储系统里面获取到的
        //比如是mysql,redis,HDFS里面获取到的
        val filterRDD: RDD[(String, Boolean)] = sc.parallelize(List("?", "!")).map(p => (p, true))
        //广播变量
        val filterRDDBroadCast = sc.broadcast(filterRDD.collect())

        val dataStream = ssc.socketTextStream("localhost", 8888)
        val wordDStream = dataStream.flatMap(_.split(","))
        val wordAndOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))
        //进行黑名单过滤
        //transform可以把DStream转变为rdd
        val filterDStream: DStream[(String, Int)] = wordAndOneDStream.transform(rdd => {
            //这儿使用了外面的变量，我们这个时候就可以想到
            //使用广播变量

            //黑名单
            val filterRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(filterRDDBroadCast.value)
            /**
              * rdd:
              * hadoop,1  => string,int
              * !,1
              * filterRDD:
              * ?,true     => string,boolean
              * !,true
              *
              * !,1  join !,true
              *
              * (!,(1,Some(true))) 凡是我们能join上的，就是我们不要的数据。
              * 大家看一下，我们不要的这些数据有什么特点呢？
              *
              * (hadoop,(1,NONE))
              * (hive,(1,NONE))
              *
              */
            val result: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filterRDD)

            // 对于filter来说 返回true就是保留下来的数据。
            result.filter(tuple => {
                //我们要的是没有join上的
                //(hive,(1,NONE))类似这样的数据是我们需要的
                tuple._2._2.isEmpty
            }).map(tuple => (tuple._1, 1))
        })
        /**
          * RDD:
          * map:
          * record
          * mapPartition:
          * partition
          * DStream:
          * transform:  => mapRDD
          * RDD
          */
        val result = filterDStream.reduceByKey(_ + _)
        result.print()
        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }

}
