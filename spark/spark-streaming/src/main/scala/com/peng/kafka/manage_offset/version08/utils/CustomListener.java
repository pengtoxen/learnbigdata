//package com.peng.kafka.manage_offset.version08.utils;
//
//import kafka.common.TopicAndPartition;
//import org.apache.spark.streaming.kafka.KafkaCluster;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import org.apache.spark.streaming.scheduler.*;
//import scala.Option;
//import scala.collection.JavaConversions;
//import scala.collection.immutable.List;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class CustomListener implements StreamingListener {
//
//    private KafkaCluster kc;
//    public scala.collection.immutable.Map<String, String> kafkaParams;
//
//    public CustomListener(scala.collection.immutable.Map<String, String> kafkaParams) {
//        this.kafkaParams = kafkaParams;
//        kc = new KafkaCluster(kafkaParams);
//    }
//
//    @Override
//    public void onStreamingStarted(StreamingListenerStreamingStarted streamingStarted) {
//
//    }
//
//    @Override
//    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
//
//    }
//
//    @Override
//    public void onReceiverError(StreamingListenerReceiverError receiverError) {
//
//    }
//
//    @Override
//    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
//
//    }
//
//    @Override
//    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
//
//    }
//
//    @Override
//    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
//
//    }
//
//    /**
//     * 当一个SparkStreaming程序运行完了以后,会触发这个方法
//     * 里面方法里面完成偏移量的提交
//     *
//     * @param batchCompleted
//     */
//    @Override
//    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
//
//        /**
//         * batchCompleted这个对象里有偏移量信息
//         *
//         * 一个批次里面是有多个task,一般你有几个分区,就会有几个task任务.
//         * 万一,比如有10个task,有8个task运行成功了,2个task运行失败了.
//         * 但是我们偏移量会被照常提交,那这样的话,会丢数据.
//         * 所以我们要判断一个事,只有所有的task都运行成功了,才提交偏移量.
//         *
//         * 10 task   5task 运行成功  5task运行失败,不让提交偏移量
//         * 会有小量的数据重复,这个是在企业里面95%的场景都是接受的.
//         * 如果是我们的公司,我们公司里面所有的实时的任务都接受有少量的数据重复.但是就是不允许丢失.
//         *
//         * 如果是运行成功的task,是没有失败的原因的（failureReason 这个字段是空的）
//         * 如果说一个task是失败了,那failureReason这个字段里面有值,会告诉你失败的原因.
//         *
//         */
//
//        //如果本批次里面有任务失败了,那么就终止偏移量提交
//        scala.collection.immutable.Map<Object, OutputOperationInfo> opsMap = batchCompleted.batchInfo().outputOperationInfos();
//
//        Map<Object, OutputOperationInfo> javaOpsMap = JavaConversions.mapAsJavaMap(opsMap);
//        for (Map.Entry<Object, OutputOperationInfo> entry : javaOpsMap.entrySet()) {
//            //failureReason不等于None(是scala中的None),说明有异常,不提交偏移量
//            if (!"None".equalsIgnoreCase(entry.getValue().failureReason().toString())) {
//                return;
//            }
//        }
//
//        long batchTime = batchCompleted.batchInfo().batchTime().milliseconds();
//
//        /**
//         * topic,分区,偏移量
//         * map结构里可能会有多个topic
//         */
//        Map<String, Map<Integer, Long>> offset = getOffset(batchCompleted);
//        for (Map.Entry<String, Map<Integer, Long>> entry : offset.entrySet()) {
//            String topic = entry.getKey();
//            Map<Integer, Long> paritionToOffset = entry.getValue();
//
//            //我只需要这儿把偏移信息放入到zookeeper就可以了.
//            for (Map.Entry<Integer, Long> p2o : paritionToOffset.entrySet()) {
//
//                Map<TopicAndPartition, Object> map = new HashMap<TopicAndPartition, Object>();
//
//                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, p2o.getKey());
//
//                map.put(topicAndPartition, p2o.getValue());
//
//                scala.collection.immutable.Map<TopicAndPartition, Object> topicAndPartitionObjectMap = TypeHelper.toScalaImmutableMap(map);
//
//                kc.setConsumerOffsets(kafkaParams.get("group.id").get(), topicAndPartitionObjectMap);
//            }
//        }
//    }
//
//    @Override
//    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
//
//    }
//
//    @Override
//    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
//
//    }
//
//    private Map<String, Map<Integer, Long>> getOffset(StreamingListenerBatchCompleted batchCompleted) {
//        Map<String, Map<Integer, Long>> map = new HashMap<>();
//
//        scala.collection.immutable.Map<Object, StreamInputInfo> inputInfoMap = batchCompleted.batchInfo().streamIdToInputInfo();
//
//        Map<Object, StreamInputInfo> infos = JavaConversions.mapAsJavaMap(inputInfoMap);
//
//        infos.forEach((k, v) -> {
//            Option<Object> optOffsets = v.metadata().get("offsets");
//            if (!optOffsets.isEmpty()) {
//                Object objOffsets = optOffsets.get();
//                if (List.class.isAssignableFrom(objOffsets.getClass())) {
//                    List<OffsetRange> scalaRanges = (List<OffsetRange>) objOffsets;
//
//                    Iterable<OffsetRange> ranges = JavaConversions.asJavaIterable(scalaRanges);
//                    for (OffsetRange range : ranges) {
//                        if (!map.containsKey(range.topic())) {
//                            map.put(range.topic(), new HashMap<>());
//                        }
//                        map.get(range.topic()).put(range.partition(), range.untilOffset());
//                    }
//                }
//            }
//        });
//
//        return map;
//    }
//}
