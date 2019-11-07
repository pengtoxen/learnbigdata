package com.peng

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 尽可能复用同一个RDD
  * 除了要避免在开发过程中对一份完全相同的数据创建多个RDD之外，在对不同的数据执行算子操作时还要尽可能地复用一个RDD。
  * 比如说，有一个RDD的数据格式是key-value类型的，另一个是单value类型的，这两个RDD的value数据是完全一样的。
  * 那么此时我们可以只使用key-value类型的那个RDD，因为其中已经包含了另一个的数据。对于类似这种多个RDD的数据有重叠或者包含的情况，
  * 我们应该尽量复用一个RDD，这样可以尽可能地减少RDD的数量，从而尽可能减少算子执行的次数。
  */
object Tuning2 {
    def main(args: Array[String]): Unit = {
        //构建sparkConf对象,设置application名称和master地址
        val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        //构建sparkContext对象,该对象非常重要,它是所有spark程序的执行入口
        val sc = new SparkContext(sparkConf)
        //读取数据文件
        //需要对名为“data.txt”的HDFS文件进行一次map操作，再进行一次reduce操作。也就是说，需要对一份数据执行两次算子操作。
        val path = "tuning/testdata/rawdata/data.text"

        sc.stop()
    }
}