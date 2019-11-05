package com.peng

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 避免创建重复的RDD
  * 通常来说，我们在开发一个Spark作业时，首先是基于某个数据源（比如Hive表或HDFS文件）创建一个初始的RDD；
  * 接着对这个RDD执行某个算子操作，然后得到下一个RDD；以此类推，循环往复，直到计算出最终我们需要的结果。
  * 在这个过程中，多个RDD会通过不同的算子操作（比如map、reduce等）串起来，这个“RDD串”，就是RDD lineage，
  * 也就是“RDD的血缘关系链”。
  *
  * 我们在开发过程中要注意：对于同一份数据，只应该创建一个RDD，不能创建多个RDD来代表同一份数据。
  *
  * 一些Spark初学者在刚开始开发Spark作业时，或者是有经验的工程师在开发RDD lineage极其冗长的Spark作业时，
  * 可能会忘了自己之前对于某一份数据已经创建过一个RDD了，从而导致对于同一份数据，创建了多个RDD。这就意味着，
  * 我们的Spark作业会进行多次重复计算来创建多个代表相同数据的RDD，进而增加了作业的性能开销
  */
object Tuning1 {
    def main(args: Array[String]): Unit = {
        //构建sparkConf对象,设置application名称和master地址
        val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        //构建sparkContext对象,该对象非常重要,它是所有spark程序的执行入口
        val sc = new SparkContext(sparkConf)
        //读取数据文件
        //需要对名为“data.txt”的HDFS文件进行一次map操作，再进行一次reduce操作。也就是说，需要对一份数据执行两次算子操作。
        val path = "tuning/testdata/rawdata/data.text"

        //错误的做法：
        //对于同一份数据执行多次算子操作时，创建多个RDD。
        //这里执行了两次textFile方法，针对同一个HDFS文件，创建了两个RDD出来，然后分别对每个RDD都执行了一个算子操作。
        //这种情况下，Spark需要从HDFS上两次加载hello.txt文件的内容，并创建两个单独的RDD；第二次加载HDFS文件以及创建RDD的性能开销，
        //很明显是白白浪费掉的。
        val rdd1: RDD[String] = sc.textFile(path)
        rdd1.map(x => x.split(","))

        val rdd2: RDD[String] = sc.textFile(path)
        rdd2.map(x => x.split(","))

        //正确的做法
        //对于一份数据执行多次算子操作时，只使用一个RDD。
        //这种写法很明显比上一种写法要好多了，因为我们对于同一份数据只创建了一个RDD，然后对这一个RDD执行了多次算子操作。
        //但是要注意到这里为止优化还没有结束，由于rdd1被执行了两次算子操作，第二次执行reduce操作的时候，
        // 还会再次从源头处重新计算一次rdd1的数据，因此还是会有重复计算的性能开销。
        //要彻底解决这个问题，必须结合“原则三：对多次使用的RDD进行持久化”，才能保证一个RDD被多次使用时只被计算一次。
        val rdd3: RDD[String] = sc.textFile(path)
        rdd3.map(x => x.split(","))
        rdd3.reduce((x, y) => {
            x + y
        })

        sc.stop()
    }
}