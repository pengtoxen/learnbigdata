package com.peng.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * 自定义sink
 * 把数据写入redis
 *
 * @author Administrator
 */
public class SinkForRedis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //步骤二：模拟数据
        ArrayList<String> data = new ArrayList<String>();
        data.add("hadoop");
        data.add("spark");
        data.add("flink");

        //步骤三：获取数据源
        DataStreamSource<String> rawData = env.fromCollection(data);

        //对数据进行组装,把string转化为tuple2<String,String>
        DataStream<Tuple2<String, String>> dataStream = rawData.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("peng", value);
            }
        });

        //创建redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

        //创建redisSink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        //添加sink
        dataStream.addSink(redisSink);

        //执行任务
        env.execute("SinkForRedis");
    }

    /**
     * 原始数据
     * ("peng", "hadoop")
     * ("peng", "spark")
     * ("peng", "flink")
     *
     * 类似于执行
     * redis 127.0.0.1:6379> LPUSH peng hadoop
     * redis 127.0.0.1:6379> LPUSH peng spark
     * redis 127.0.0.1:6379> LPUSH peng flink
     *
     * 结果
     * redis 127.0.0.1:6379> lrange peng 0 100
     * 1) "flink"
     * 2) "spark"
     * 3) "hadoop"
     */
    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        //表示从接收的数据中获取需要操作的redis key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        //表示从接收的数据中获取需要操作的redis value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }

        /**
         * 执行redis的lpush命令
         * LPUSH peng value
         *
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }
}
