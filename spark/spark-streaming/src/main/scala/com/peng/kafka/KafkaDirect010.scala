package com.peng.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 0.10版本
  * 消费者的偏移量默认存储到了kafka里面
  * 只支持direct的方式
  */
object KafkaDirect010 {
	
	def main(args: Array[String]): Unit = {
		
		Logger.getLogger("org").setLevel(Level.ERROR)
		
		//步骤一：获取配置信息
		val conf = new SparkConf()
		conf.setAppName("test")
		conf.setMaster("local[3]")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		val ssc = new StreamingContext(conf, Seconds(5))

		val brokers = "node1:9092"

		//注意,这个也就是我们的消费者的名字
		val groupId = "peng" 

		val topicsSet = "flink".split(",").toSet

		/**
		  * 今天我一天就只干了一个事,就是了一篇文章,总结大家平时开发Spark会出现哪些问题.
		  * 其实一个问题,就是这儿的这个问题.
		  * 我们KAFKA里面需要有三个地方设置消息的大小.
		  *
		  * Producer  -》 broker   -》 consumer
		  * 1M,8M         1M           1M,
		  */
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> brokers,
			"group.id" -> groupId,

			//SparkStreaming消费的kafka的一条消息,最大可以多大?
			//默认是1M,比如可以设置为10M,生产里面一般都是设置10M.
			"fetch.message.max.bytes" -> "20971520000",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer]
		)

		//步骤二：获取数据源
		val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
			ssc,
			LocationStrategies.PreferConsistent,
			ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

		val result = stream.map(_.value()).flatMap(_.split(","))
		  .map((_, 1))
		  .reduceByKey(_ + _)

		result.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}
