package consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest {
    public static void main(String[] args) {
        //步骤一：设置参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop1:9092");
        props.put("group.id", "peng");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //不要设置得太长，不然coordinator服务器不太容易发现你的
        //消费宕机了。
        props.put("heartbeat.interval.ms", 1000);
        //多久没发送心跳认为超时
        props.put("session.timeout.ms", 10 * 1000);

        props.put("max.poll.interval.ms", 30 * 1000); // 如果30秒才去执行下一次poll
        // 如果说你的消费的吞吐量特别大，此时可以适当提高一些
        props.put("max.poll.records", 1000);
        // 不要去回收那个socket连接
        props.put("connection.max.idle.ms", -1);

        // 开启自动提交，他只会每隔一段时间去提交一次offset
        // 如果你每次要重启一下consumer的话，他一定会把一些数据重新消费一遍
        props.put("enable.auto.commit", "true");
        // 每次自动提交offset的一个时间间隔
        props.put("auto.commit.ineterval.ms", "1000");

        // 每次重启都是从最早的offset开始读取，不是接着上一次
        /*
         *
         * earliest
         * 		当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         * 		topica -> partition0:1000
         * 				  partitino1:2000
         *
         *
         *
         * latest
         * 		当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从当最新的数据开始消费
         * 	   offset 100000
         *
         *
         * 	   第二种
         * 	none
         * 		topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         *
         */
        props.put("auto.offset.reset", "latest");
        //
        //步骤二：创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //步骤三：指定消费的主题
        consumer.subscribe(Arrays.asList("testkaikeba"));

        try {
            while (true) {
                //步骤四：不断的消费数据
                ConsumerRecords<String, String> records = consumer.poll(3000); // 超时时间
                //步骤五：对消费到的数据，进行业务的处理。一次消费多条数据。
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject order = JSONObject.parseObject(record.value());
                    System.out.println(order.toString() + " ,userId " + order.getString("userId"));
                }
            }
        } catch (Exception e) {

        }
    }
}

