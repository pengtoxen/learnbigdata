package com.peng.state.keyed_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
import java.util.Collections;

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
public class AverageWithListState {


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

        keyedStream.flatMap(new ListStateImp()).print();

        env.execute("AverageWithListState");
    }
}

class ListStateImp extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    /**
     * 我们的数据源那儿的每个key都会有自己的一个ValueState
     * Tuple2<String, Long>
     * String:key键
     * Long:value的和
     * <p>
     * 不同key的数据是不会混乱的，因为每个key都有自己的state
     */
    private ListState<Tuple2<String, Long>> listState;

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
        ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>(
                //状态的名字
                "average",
                //状态存储的数据类型
                Types.TUPLE(Types.STRING, Types.LONG));

        //把状态赋值给listState
        listState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> element, Collector<Tuple2<String, Double>> out) throws Exception {

        //listState是集合
        Iterable<Tuple2<String, Long>> currentState = listState.get();

        //第一次进来currentState是null,赋初值(就是空集合)
        if (currentState == null) {
            listState.addAll(Collections.emptyList());
        }

        //往集合添加数据
        listState.add(Tuple2.of(element.f0, element.f1));

        //将listState转换为ArrayList,方便迭代计算
        ArrayList<Tuple2<String, Long>> itemList = Lists.newArrayList(listState.get());

        //数据超过3个,计算平均值
        if (itemList.size() >= 3) {
            long count = 0L;
            long sum = 0L;
            for (Tuple2<String, Long> ele : itemList) {
                count++;
                sum += ele.f1;
            }
            //平均值
            double avg = (double) sum / count;
            //输出到下游
            out.collect(Tuple2.of(element.f0, avg));
            //计算完成后,清空listState
            listState.clear();
        }
    }
}