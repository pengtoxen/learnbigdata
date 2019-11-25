package com.peng.report.source;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 模拟数据源
 *
 * @author Administrator
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        //指定kafka broker地址
        prop.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //指定key value的序列化方式
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        prop.put("acks", "-1");

        //指定topic名称
        String topic = "flink-example-report";

        //创建producer链接
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //数据格式:
        //{"dt":"2018-01-01 10:11:22","type":"shelf","username":"shenhe1","area":"AREA_US"}

        while (true) {
            //模拟数据
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"type\":\"" + getRandomType() + "\",\"username\":\"" + getRandomUsername() + "\",\"area\":\"" + getRandomArea() + "\"}";
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(500);
        }
        //关闭链接
        //producer.close();
    }

    private static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    private static String getRandomArea() {
        String[] types = {"AREA_US", "AREA_CT", "AREA_AR", "AREA_IN", "AREA_ID"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getRandomType() {
        String[] types = {"s1", "s2", "s3", "s4", "s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getRandomUsername() {
        String[] types = {"shenhe1", "shenhe2", "shenhe3", "shenhe4", "shenhe5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
