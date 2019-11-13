package com.peng.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器
 *
 * @author Administrator
 */
public class PhonePartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        //TODO nothing
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //得到topic的partitions信息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        //将电话是18658318156或者13058898058的消息放到最后一个分区中
        if ("18658318156".equals(key.toString()) || "13058898058".equals(key.toString())) {
            //放到最后一个分区中
            return numPartitions - 1;
        }
        String phoneNum = key.toString();
        return phoneNum.substring(0, 3).hashCode() % (numPartitions - 1);
    }

    @Override
    public void close() {
        //TODO nothing
    }
}
