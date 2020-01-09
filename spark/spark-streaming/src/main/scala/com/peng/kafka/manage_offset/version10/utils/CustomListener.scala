package com.peng.kafka.manage_offset.version10.utils

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, OffsetRange}
import org.apache.spark.streaming.scheduler._

import scala.collection.JavaConversions

/**
  * 自定义监听器
  *
  * @param stream
  */
class CustomListener(var stream: InputDStream[ConsumerRecord[String, String]]) extends StreamingListener {

	override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit =
		super.onStreamingStarted(streamingStarted)

	override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit =
		super.onReceiverStarted(receiverStarted)

	override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit =
		super.onReceiverError(receiverError)

	override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit =
		super.onReceiverStopped(receiverStopped)

	override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {

		var isHasError = false;
		val infos: Map[Int, OutputOperationInfo] = batchSubmitted.batchInfo.outputOperationInfos

		val intToInfo: util.Map[Int, OutputOperationInfo] = JavaConversions.mapAsJavaMap(infos)

		for (kv <- infos) {
			if (!"None".equalsIgnoreCase(kv._2.failureReason.toString)) {
				isHasError = true
			}
		}

		//如果没有异常,那么就提交offset
		if (!isHasError) {

			val info: Map[Int, StreamInputInfo] = batchSubmitted.batchInfo.streamIdToInputInfo

			var offsetRangesTmp: List[OffsetRange] = null;
			var offsetRanges: Array[OffsetRange] = null;

			for (k <- info) {
				val offset: Option[Any] = k._2.metadata.get("offsets")

				if (offset.isDefined) {
					try {
						val offsetValue = offset.get
						offsetRangesTmp = offsetValue.asInstanceOf[List[OffsetRange]]
						offsetRanges = offsetRangesTmp.toSet.toArray;
					} catch {
						case e: Exception => println(e)
					}
				}
			}

			if (offsetRanges != null) {

				//自动帮我们把偏移量信息存储到了kafka里面方便很多了
				stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges);
			}

		}
	}


	override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {

	}

	override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

	}

	override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit =
		super.onOutputOperationStarted(outputOperationStarted)

	override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit =
		super.onOutputOperationCompleted(outputOperationCompleted)
}
