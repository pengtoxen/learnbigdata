package com.peng.partitioner;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

/**
 * 设置自定义分区器,发送数据到对应的broker
 *
 * @author Administrator
 */
public class PartitionerProducer {

    private static final String[] PHONE_NUMS = new String[]{
            "18658318156", "10000", "13058898058", "13700000003", "13700000004",
            "13058898058", "18658318156", "13675873032", "13675873032",
            "17600000009", "10000", "13675873032"
    };

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        //这个参数，目的就是为了获取kafka集群的元数据
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //设置分区器
        props.put("partitioner.class", "com.peng.partitioner.PhonePartitioner");
        //设置序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        int count = 0;
        int length = PHONE_NUMS.length;

        while (count < 10) {
            Random rand = new Random();
            String phoneNum = PHONE_NUMS[rand.nextInt(length)];
            ProducerRecord<String, String> record = new ProducerRecord<>("diy-partitioner2", phoneNum, phoneNum);
            //发送消息后获取元数据信息
            RecordMetadata metadata = producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("消息发送成功");
                    } else {
                        //公司里面比较严谨的项目
                        //还会有备用的链路
                        //（mysql，redis)
                        System.out.println("做其他处理");
                    }
                }
            }).get();
            String result = "phone [" + record.value() + "] has been sent to partition " + metadata.partition();
            System.out.println(result);
            Thread.sleep(500);
            count++;
        }
        producer.close();
    }
}
