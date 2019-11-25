package com.peng.report.core;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.peng.report.function.MySumFunction;
import com.peng.report.watermark.MyWaterMark;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * 统计的过去的3秒不同类型,每个大区的有效视频数量
 * 报表：就是要计算一些指标
 *
 * @author Administrator
 */
public class DataReport {

    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置checkpoint保存方式RocksDB
        //env.setStateBackend(new RocksDBStateBackend("hdfs:/hadoop1:9000/xxx"));

        //设置事件时间作为消息的时间(消息产生的时间)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从Kafka里面读取数据 消费者 数据源需要kafka
        //topic的取名还是有讲究的，最好就是让人根据这个名字就能知道里面有什么数据。
        //xxxx_xxx_xxx_xxx
        String topic = "flink-example-report";
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        consumerProperties.put("group.id", "flink_example_report_consumer");

        //读取Kafka里面对数据
        //{"dt":"2019-11-24 21:19:47","type":"child_unshelf","username":"shenhe1","area":"AREA_ID"}
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                consumerProperties);

        //kafka作为source
        DataStreamSource<String> kafkaStream = env.addSource(consumer);

        Logger logger = LoggerFactory.getLogger(DataReport.class);

        //对数据进行处理
        SingleOutputStreamOperator<Tuple3<Long, String, String>> preData = kafkaStream.map(new MapFunction<String, Tuple3<Long, String, String>>() {

            /**
             * Long:time 事件时间
             * String: type 视频类别
             * String: area 地区
             * @return
             * @throws Exception
             */
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {

                //解析kafka里的json字符串
                JSONObject jsonObject = JSON.parseObject(line);
                //时间字段
                String dt = jsonObject.getString("dt");
                //类别字段
                String type = jsonObject.getString("type");
                //地区字段
                String area = jsonObject.getString("area");
                //时间默认值
                long time = 0;
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //格式化为时间戳
                    time = sdf.parse(dt).getTime();
                } catch (ParseException e) {
                    logger.error("时间解析失败，dt:" + dt, e.getCause());
                }

                return Tuple3.of(time, type, area);
            }
        });

        /**
         * 过滤无效的数据
         */
        SingleOutputStreamOperator<Tuple3<Long, String, String>> filterData = preData.filter(new FilterFunction<Tuple3<Long, String, String>>() {
            @Override
            public boolean filter(Tuple3<Long, String, String> tuple3) throws Exception {
                return tuple3.f0 != 0;
            }
        });


        /**
         * 收集迟到太久的数据
         */
        OutputTag<Tuple3<Long, String, String>> outputTag = new OutputTag<Tuple3<Long, String, String>>("delay-date") {
        };

        /**
         * 进行窗口的统计操作
         * 统计的过去的3秒不同类型,每个大区的有效视频数量
         */
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData = filterData.assignTimestampsAndWatermarks(new MyWaterMark())
                //按照第一个字段,第二个字段分组
                .keyBy(1, 2)
                //滚动窗口3秒
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //处理迟到太多的数据
                .sideOutputLateData(outputTag)
                //自定义的统计方法
                .apply(new MySumFunction());

        /**
         * 收集到延迟太多的数据，业务里面要求写到Kafka
         */
        SingleOutputStreamOperator<String> sideOutput = resultData.getSideOutput(outputTag).map(new MapFunction<Tuple3<Long, String, String>, String>() {

            /**
             * 将tuple3类型的数据转换成string类型,存储到kafka
             * @param line
             * @return
             * @throws Exception
             */
            @Override
            public String map(Tuple3<Long, String, String> line) throws Exception {
                return line.toString();
            }
        });


        String outputTopic = "flink-example-report-delay-data";
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(outputTopic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                producerProperties);
        sideOutput.addSink(producer);

        /**
         * 业务里面需要吧数据写到ES里面
         * 而我们公司是需要把数据写到kafka
         */
        resultData.print();

        env.execute("DataReport");
    }
}
