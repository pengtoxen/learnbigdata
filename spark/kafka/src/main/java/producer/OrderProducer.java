package producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class OrderProducer {
    /**
     * 创建生产者
     * @return
     */
    public static KafkaProducer<String, String>  getProducer(){
        Properties props = new Properties();

        props.put("bootstrap.servers", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "-1");
        props.put("retries", 3);//5-10次
        props.put("retry.backoff.ms",2000);

        props.put("compression.type","lz4");

        props.put("buffer.memory", 33554432);
        props.put("batch.size", 323840);
        props.put("linger.ms", 100);
        props.put("max.request.size",1048576);
        props.put("request.timeout.ms",3000);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        return producer;
    }

    /**
     * 创建消息
     * @return
     */
    public static  JSONObject createRecord(){
        JSONObject order = new JSONObject();
        order.put("userId","123");
        order.put("orderId","123a");
        order.put("amount",1000.0);
        order.put("operator","pay");
        return order;
    }


    public static void main(String[] args) throws Exception {
        //步骤一
        KafkaProducer<String, String> producer = getProducer();
        //步骤二
        JSONObject order = createRecord();
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                "testkaikeba", order.getString("userId"),order.toString());


        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if(exception == null) {
                    System.out.println("消息发送成功");
                } else {
                   //公司里面比较严谨的项目
                    //还会有备用的链路
                    //（mysql，redis)
                    System.out.println("做其他处理");
                }
            }

        });
        Thread.sleep(100000);
        // 这是同步发送的模式 try
		///producer.send(record).get();
        // 你要一直等待人家后续一系列的步骤都做完，发送消息之后
        // 有了消息的回应返回给你，你这个方法才会退出来
        /**
         * 步骤五：关闭链接
         */
        producer.close();
    }

}

