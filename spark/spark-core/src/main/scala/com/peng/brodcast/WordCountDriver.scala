package com.peng.brodcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark中分布式执行的代码需要==传递到各个Executor的Task上运行.
  * 对于一些只读、固定的数据(比如从DB中读出的数据),每次都需要Driver广播到各个Task上，这样效率低下。
  * 广播变量允许将变量只广播给各个Executor,该Executor上的各个Task再从所在节点的BlockManager获取变量，
  * 而不是从Driver获取变量，以减少通信的成本，减少内存的占用，从而提升了效率.
  */
object WordCountDriver {
	def main(args: Array[String]): Unit = {
		//1.构建sparkConf对象,设置application名称和master地址
		val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
		//2.构建sparkContext对象,该对象非常重要,它是所有spark程序的执行入口
		//内部会构建DAGScheduler和TaskScheduler对象
		val sc = new SparkContext(sparkConf)
		//设置日志输出级别
		sc.setLogLevel("warn")
		//3.读取数据文件
		val data: RDD[String] = sc.textFile("spark-core/testdata/rawdata/wordcount/data.txt")
		//4.切分每一行,获取所有单词
		val words: RDD[String] = data.flatMap(x => x.split(" "))
		//5.每个单词计为1
		val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
		//6.相同单词出现的1累加
		val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)

        /**
          * 通过调用sparkContext对象的broadcast方法把数据广播出去
          * 1、不能将一个RDD使用广播变量广播出去
          * 2、广播变量只能在Driver端定义，不能在Executor端定义
          * 3、在Driver端可以修改广播变量的值，在Executor端无法修改广播变量的值
          * 4、如果executor端用到了Driver的变量，如果不使用广播变量在Executor有多少task就有多少Driver端的变量副本
          * 5、如果Executor端用到了Driver的变量，如果使用广播变量在每个Executor中只有一份Driver端的变量副本
          */
		val word = "spark"
		val broadcast = sc.broadcast(word)

        //在executor中通过调用广播变量的value属性获取广播变量的值
		val filterResult = result.filter(x => x._1.equals(broadcast.value))

		//7.收集数据打印
		val finalResult: Array[(String, Int)] = filterResult.collect()
		finalResult.foreach(println)
		//8.关闭sc
		sc.stop()
	}
}
