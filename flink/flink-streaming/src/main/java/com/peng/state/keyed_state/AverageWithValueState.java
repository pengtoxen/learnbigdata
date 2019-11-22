package com.peng.state.keyed_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

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
public class AverageWithValueState {


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

        keyedStream.flatMap(new ValueStateImp()).print();

        env.execute("KeyedStateTask");
    }
}

class ValueStateImp extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    /**
     * 我们的数据源那儿的每个key都会有自己的一个ValueState
     * Tuple2<String, Long, Long>
     * String:key键
     * Long:key键出现的次数
     * Long:value的和
     *
     * 不同key的数据是不会混乱的，因为每个key都有自己的state
     */
    private ValueState<Tuple3<String, Long, Long>> countAndSum;

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
        ValueStateDescriptor<Tuple3<String, Long, Long>> descriptor = new ValueStateDescriptor<>(
                //状态的名字
                "average",
                //状态存储的数据类型
                Types.TUPLE(Types.STRING, Types.LONG, Types.LONG));

        //把状态赋值给countAndSum
        countAndSum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> element, Collector<Tuple2<String, Double>> out) throws Exception {
        //当前key出现的次数，和对应的value的总值
        Tuple3<String, Long, Long> currentState = countAndSum.value();
        //第一次进来currentState是null,赋初值
        if (currentState == null) {
            currentState = Tuple3.of(element.f0, 0L, 0L);
        }

        //出现的key次数累加1
        currentState.f1 += 1;
        //key对应value累加值
        currentState.f2 += element.f1;

        //更新一下状态
        countAndSum.update(currentState);

        if (currentState.f1 >= 3) {
            //计算平均值
            double avg = (double) currentState.f2 / currentState.f1;
            out.collect(Tuple2.of(element.f0, avg));
            //计算完成后,清除历史状态
            countAndSum.clear();
        } else {
            //<3次不做处理
        }
    }
}