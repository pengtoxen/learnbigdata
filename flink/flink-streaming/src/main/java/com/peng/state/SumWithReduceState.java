package com.peng.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 实现key相同的value累加功能
 *
 * @author Administrator
 */
public class SumWithReduceState {


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

        keyedStream.flatMap(new ReduceStateImp()).print();

        env.execute("SumWithReduceState");
    }
}

/**
 * ReducingState<T> ：这个状态为每一个key保存一个聚合之后的值
 *
 * @author Administrator
 */
class ReduceStateImp extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

    /**
     * 用于保存每一个 key 对应的 value 的总值
     */
    private ReducingState<Long> sumState;

    /**
     * 每个task执行一次open
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<Long>(
                //状态的名字
                "sum",
                //聚合函数
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return value1 + value2;
                    }
                    //状态存储的数据类型
                }, Long.class);
        sumState = getRuntimeContext().getReducingState(descriptor);
    }

    /**
     * element来的每一条数据
     * out输出结果
     *
     * @param element
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<String, Long> element, Collector<Tuple2<String, Long>> out) throws Exception {

        //将数据放到状态中
        //get() 获取状态值
        //add() 更新状态值，将数据放到状态中
        //clear() 清除状态
        sumState.add(element.f1);

        //输出到下游
        out.collect(Tuple2.of(element.f0, sumState.get()));
    }
}
