package com.peng.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * IN:输入的数据类型
 * Tuple2<String, Long>
 * String:key键
 * Long:value值
 * OUT输出的数据类型
 * Tuple2<String, Double>
 * String:key键
 * Double:value平均值
 *
 * @author Administrator
 */
public class AverageWithMapState {


    public static void main(String[] args) throws Exception {

        //获取环境变量
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置checkpoint
        //env.setStateBackend(new RocksDBStateBackend("xxx"));

        //模拟数据
        ArrayList<Tuple2<String, Long>> mockData = new ArrayList<>();
        mockData.add(Tuple2.of("K1", 10L));
        mockData.add(Tuple2.of("K2", 10L));
        mockData.add(Tuple2.of("K2", 10L));
        mockData.add(Tuple2.of("K1", 25L));
        mockData.add(Tuple2.of("K1", 10L));
        mockData.add(Tuple2.of("K2", 10L));
        mockData.add(Tuple2.of("K2", 10L));
        mockData.add(Tuple2.of("K2", 100L));
        //获取数据源
        DataStreamSource<Tuple2<String, Long>> dataStream = env.fromCollection(mockData);
        //按key分组才会有keyedState
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = dataStream.keyBy(0);

        keyedStream.flatMap(new MapStateImp()).print();

        env.execute("AverageWithMapState");
    }
}

class MapStateImp extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    /**
     * 我们的数据源那儿的每个key都会有自己的一个ValueState
     * MapState<String, Long>
     * String:key键(UUID)
     * Long:value值
     * <p>
     * 不同key的数据是不会混乱的，因为每个key都有自己的state
     */
    private MapState<String, Long> mapState;

    /**
     * 这个方法就只会执行一次，用来初始化的时候使用的
     * 每个task执行一次
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        //把状态注册一下，让flink帮我们管理状态
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                //状态的名字
                "average",
                //状态存储的数据类型
                String.class, Long.class);

        //把状态赋值给mapState
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> element, Collector<Tuple2<String, Double>> out) throws Exception {

        //uuid作为key,可以避免key相同的元素互相覆盖
        mapState.put(UUID.randomUUID().toString(), element.f1);

        //将mapState转换为ArrayList,方便迭代计算
        ArrayList<Long> itemList = Lists.newArrayList(mapState.values());

        //数据超过3个,计算平均值
        if (itemList.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Long ele : itemList) {
                count++;
                sum += ele;
            }
            //平均值
            double avg = (double) sum / count;
            //输出到下游
            out.collect(Tuple2.of(element.f0, avg));
            mapState.clear();
        }
    }
}