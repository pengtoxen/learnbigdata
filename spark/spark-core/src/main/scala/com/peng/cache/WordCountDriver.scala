package com.peng.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
          * 缓存result
          * 对RDD设置缓存成可以调用rdd的2个方法： 一个是cache，一个是persist
          * 调用上面2个方法都可以对rdd的数据设置缓存，但不是立即就触发缓存执行，后面需要有action，才会触发缓存的执行。
          *
          * cache方法和persist方法区别：
          * cache:   默认是把数据缓存在内存中，其本质就是调用persist方法；
          * persist：可以把数据缓存在内存或者是磁盘，有丰富的缓存级别，这些缓存级别都被定义在StorageLevel这个object中
          * 有很多的缓存级别,具体查看StorageLevel
          *
          * 缓存的目的是避免重复计算rdd,当某个rdd计算失败的时候,会根据lineage从头开始计算.如果父rdd已经缓存了结果数据,
          * 那么就可以直接拿父rdd的结果重新计算
          *
          * 一个application应用程序结束之后，对应的缓存数据也就自动清除
          * 也可以调用rdd的unpersist方法手动清除
          */
        result.cache()
        //result.persist()

        //7.按照单词出现的次数降序排列,第二个参数默认是true表示升序,false表示降序
        val sortedRDD: RDD[(String, Int)] = result.sortBy(x => x._2, false)
        //8.收集数据打印
        val finalResult: Array[(String, Int)] = sortedRDD.collect()
        finalResult.foreach(println)
        //9.关闭sc
        sc.stop()
    }
}
