package com.peng.etl.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.peng.etl.source.RedisSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * 数据清洗
 *
 * @author Administrator
 */
public class DataClean {

    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //我们是从Kafka里面读取数据，所以这儿就是topic有多少个partition，那么就设置几个并行度
        env.setParallelism(3);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //注释，我们这儿其实需要设置state backed类型，我们要把checkpoint的数据存储到RocksDB里面
        //env.setStateBackend(new RocksDBStateBackend("hdfs:/hadoop1:9000/xxx"));

        //从Kafka里面读取数据 消费者 数据源需要kafka
        //topic的取名还是有讲究的，最好就是让人根据这个名字就能知道里面有什么数据。
        //xxxx_xxx_xxx_xxx
        String topic = "flink-example-etl";
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        consumerProperties.put("group.id", "flink_example_etl_consumer");

        /**
         * String topic, 主题
         * KafkaDeserializationSchema<T> deserializer,
         * Properties props
         *
         * 数据格式:
         * {"dt":"2019-11-24 19:54:23","countryCode":"PK","data":[{"type":"s4","score":0.8,"level":"C"},{"type":"s5","score":0.2,"level":"C"}]}
         */
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                //主题名称
                topic,
                //反序列化器
                new SimpleStringSchema(),
                //配置信息
                consumerProperties);

        //kafka作为source
        //第一个流
        DataStreamSource<String> kafkaStream = env.addSource(consumer);

        //redis作为source
        //第二个流
        
        //设置为广播变量
        //这里需要注意广播变量和spark中的不一样
        //因为kafka流有3个并行度,redis流只有1个并行度
        //他们connect的时候,可能会导致3个kafka流竞争和redis流合并的情况
        //会导致有一些kafka流获取不到数据
        //为了解决这个问题,设置广播变量,相当于将redis流复制3份
        //这样每一个kafka流都能和redis流connect
        DataStream<HashMap<String, String>> redisStream = env.addSource(new RedisSource()).broadcast();

        SingleOutputStreamOperator<String> etlData = kafkaStream.connect(redisStream)
                .flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
                    
                    HashMap<String, String> areaMap = new HashMap<String, String>();

                    //里面处理的是kafka的数据
                    @Override
                    public void flatMap1(String line, Collector<String> out) throws Exception {
                        //解析kafka拿到的json串
                        JSONObject jsonObject = JSONObject.parseObject(line);
                        //时间字段
                        String dt = jsonObject.getString("dt");
                        //国家编码字段
                        String countryCode = jsonObject.getString("countryCode");
                        //可以根据countryCode获取地区的名字
                        String area = areaMap.get(countryCode);
                        //解析json数组
                        JSONArray data = jsonObject.getJSONArray("data");
                        for (int i = 0; i < data.size(); i++) {
                            JSONObject dataObject = data.getJSONObject(i);
                            System.out.println("大区：" + area);
                            dataObject.put("dt", dt);
                            dataObject.put("area", area);
                            //下游获取到数据的时候，也就是一个json格式的数据
                            out.collect(dataObject.toJSONString());
                        }
                    }

                    //里面处理的是redis里面的数据
                    @Override
                    public void flatMap2(HashMap<String, String> map, Collector<String> collector) throws Exception {
                        System.out.println(map.toString());
                        areaMap = map;
                    }
                });

        //打印数据
        etlData.print().setParallelism(1);

        /**
         * 清洗后的数据输出回kafka
         *
         * String topicId,
         * SerializationSchema<IN> serializationSchema,
         * Properties producerConfig)
         */
        //String outputTopic="allDataClean";
        //Properties producerProperties = new Properties();
        //producerProperties.put("bootstrap.servers","192.168.167.254:9092");
        //FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(outputTopic,
        //        new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
        //        producerProperties);
        //
        ////搞一个Kafka的生产者
        //etlData.addSink(producer);

        env.execute("DataClean");
    }
}
