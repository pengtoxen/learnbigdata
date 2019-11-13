package com.peng.partitioner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Administrator
 */
public class PartitionerConsumer {

    private static KafkaConsumer<String, String> createConsumer() {
        //步骤一：设置参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092");
        props.put("group.id", "diy-partitioner");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //不要设置得太长，不然coordinator服务器不太容易发现你的
        //消费宕机了
        props.put("heartbeat.interval.ms", 1000);
        //多久没发送心跳认为超时
        props.put("session.timeout.ms", 10 * 1000);

        //如果30秒才去执行下一次poll
        props.put("max.poll.interval.ms", 30 * 1000);
        //如果说你的消费的吞吐量特别大，此时可以适当提高一些
        props.put("max.poll.records", 1000);
        //不要去回收那个socket连接
        props.put("connection.max.idle.ms", -1);

        //开启自动提交，他只会每隔一段时间去提交一次offset
        //如果你每次要重启一下consumer的话，他一定会把一些数据重新消费一遍
        //为避免重复消费,一般会自定义提交offset(kafka0.8版)
        props.put("enable.auto.commit", "true");

        // 每次自动提交offset的一个时间间隔
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");

        //步骤二：创建消费者
        return new KafkaConsumer<String, String>(props);
    }

    /**
     * 这儿有一个main，然后我的这个代码如果运行一次，那么就是一个消费者。
     * 比如我把我的这个代码 分别部署在Hadoop1 Hadoop2 Hadoop3
     * Hadoop1启动这个代码  一个
     * <p>
     * 或者我们在同一台服务器上启动多个消费也是可以的
     * Hadoop2启动这个  一个
     * Hadoop3启动这个代码  一个
     * <p>
     * 就会有三个消费者。
     * <p>
     * 每个消费者其实是有名字的。但是这个名字，是自动分配，而且都不一样UUID。
     *
     * @param args
     */
    public static void main(String[] args) {
        ExecutorService threadPool = Executors.newFixedThreadPool(20);
        KafkaConsumer<String, String> consumer = createConsumer();

        //步骤三：指定消费的主题
        consumer.subscribe(Arrays.asList("diy-partitioner2"));

        try {
            while (true) {
                //步骤四：不断的消费数据
                //超时时间
                ConsumerRecords<String, String> records = consumer.poll(3000);
                //步骤五：对消费到的数据，进行业务的处理。一次消费多条数据。
                for (ConsumerRecord<String, String> record : records) {
                    //放到线程池里面去消费就可以了。
                    threadPool.submit(new ConsumerTask(record.key()));
                }
            }
        } catch (Exception e) {

        }
    }

    public static class ConsumerTask implements Runnable {
        private String record;

        ConsumerTask(String record) {
            this.record = record;
        }

        @Override
        public void run() {
            System.out.println("做一些业务的处理：" + record);
        }
    }

}

