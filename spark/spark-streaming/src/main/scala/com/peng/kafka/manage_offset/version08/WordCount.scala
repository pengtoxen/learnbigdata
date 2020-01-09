//package com.peng.kafka.manage_offset.version08
//
//import com.peng.kafka.manage_offset.version08.utils.{CustomListener, KafkaManager}
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * offset的管理的问题
//  * 目前我们的代码里面是没有去管理的,就会有有两个问题
//  * 1.下一次我程序启动的时候,我从哪儿去消费,你要是不知道,是不会就会丢数据？
//  * 企业里面一般会容忍你重复消费数据,但是基本很少允许你丢数据.
//  * 2.那你的程序是否消费延迟,是不是也就是个未知数,程序就相当于裸奔,这个程序说白了就是一点意义都没有.
//  * 甚至,如果说企业里面要求比较,需要我们数据要实现仅一次的语义.
//  *
//  * checkpoint也是可以保存偏移量信息的
//  * 这种方式很简单,为什么不用呢?
//  *
//  * 不同分区保存不同的偏移量信息
//  * topicA/p0,p1,p2 -> (topicA(p0-100,p1-90,p2-110))
//  * 但是p0,p1,p2的值是根据jar包信息计算出来的
//  *
//  * 所以如果只是重启,没问题,但是如果项目重新打包后,p0的值就变了,所以找不到之前的偏移量
//  * val conf=new SparkConf();
//  * val ssc=new StreamingContext(conf,Seconds(10s))
//  * ssc.checkpoint("hdfs://peng/streaming/checkpoint/wordcount/k-v(元数据信息)")
//  */
//object WordCount {
//
//	def main(args: Array[String]): Unit = {
//
//		val conf = new SparkConf().setMaster("local[3]").setAppName("WordCount")
//		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		val ssc = new StreamingContext(conf, Seconds(10))
//
//		val brokers = "node1:9092"
//		val topics = "streaming"
//
//		//注意,这个也就是我们的消费者的名字
//		val groupId = "streaming_consumer"
//
//		val topicsSet = topics.split(",").toSet
//
//		val kafkaParams = Map[String, String](
//			"metadata.broker.list" -> brokers,
//			"group.id" -> groupId
//		)
//
//		//关键步骤一：设置监听器,帮我们完成偏移量的提交
//		/**
//		  * 监听器的作用就是,我们每次运行完一个批次,就帮我们提交一次偏移量.
//		  * 提交偏移量的时效性得到保证
//		  *
//		  * kafka默认的提交变量量是每隔1段时间提交,所以在我们消费了部分数据后
//		  * 还没有提交偏移量的时候挂了,那么重启后又开始从上一次的偏移量开始消费
//		  */
//		ssc.addStreamingListener(new CustomListener(kafkaParams))
//
//		//关键步骤二:创建对象,然后通过这个对象获取到上次的偏移量,然后获取到数据流
//		val km = new KafkaManager(kafkaParams)
//
//		//1.获取到流,这个流里面是offset的信息的
//		//如果不直接运行foreachRDD,那么会丢失offset信息
//		val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
//			ssc, kafkaParams, topicsSet)
//
//		//执行map算子后
//		//注意:!!!offset的信息就会丢失了
//		//就没法提交偏移量
//		val result = messages.map(_._2)
//		  .flatMap(_.split(","))
//		  .map((_, 1))
//		  .reduceByKey(_ + _)
//
//		//2.直接对上面获取的流做foreachRDD的操作
//		//这样子仍然保存了偏移量信息
//		result.foreachRDD(rdd => {
//
//			//缺点就是,所有的业务逻辑都要在这儿实现
//			//不是说不行,也是可以的.
//			//但是大家会发现,那我们的DSteam编程,就变成了SparkCore编程
//			//如果功能就是单词计数,问题也不大.
//			//但是如果你想使用一些DStream特有的算子,你就用不了
//			//UpdateStateBykey,mapWithState,transform,Window(窗口)
//			//实际上这个时候,里面已经没有offset的信息了
//			//那你就没办法提交offset
//			rdd.foreach(line => {
//				println(line._1 + "  " + line._2)
//				println("-============================================")
//
//				//代码到这儿 应该要提交一下偏移量了.
//				//确实是可以实现提交offset的功能的.
//				//但是这样操作不优雅
//				//所以可以用监听器,来监听这个流
//				//当流执行完成后,监听器来提交偏移量
//			})
//		})
//
//		ssc.start()
//		ssc.awaitTermination()
//		ssc.stop()
//	}
//}
//
