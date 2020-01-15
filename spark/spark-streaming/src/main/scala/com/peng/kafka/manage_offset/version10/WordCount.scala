package com.peng.kafka.manage_offset.version10

import com.peng.kafka.manage_offset.version10.utils.CustomListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
	def main(args: Array[String]): Unit = {

		//步骤一:获取配置信息
		val conf = new SparkConf().setAppName("WordCount").setMaster("local[3]")
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val ssc = new StreamingContext(conf, Seconds(5))

		val brokers = "node1:9092"
		val topics = "peng"

		//注意,这个也就是我们的消费者的名字
		val groupId = "peng_consumer"

		val topicsSet = topics.split(",").toSet

		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> brokers,
			"group.id" -> groupId,
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer]
		)

		//步骤二:获取数据源
		val DStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
			ssc,
			LocationStrategies.PreferConsistent,
			ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

		//设置监听器
		ssc.addStreamingListener(new CustomListener(DStream))

		val result = DStream.map(_.value()).flatMap(_.split(","))
		  .map((_, 1))
		  .reduceByKey(_ + _)

		result.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}
}