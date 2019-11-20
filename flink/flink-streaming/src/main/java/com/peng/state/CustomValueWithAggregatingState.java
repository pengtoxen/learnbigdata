package com.peng.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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
 * 运行结果
 * 1> (K1,Contains：10)
 * 1> (K2,Contains：10)
 * 1> (K2,Contains：10 and 10)
 * 1> (K1,Contains：10 and 25)
 * 1> (K1,Contains：10 and 25 and 10)
 * 1> (K2,Contains：10 and 10 and 10)
 * 1> (K2,Contains：10 and 10 and 10 and 10)
 * 1> (K2,Contains：10 and 10 and 10 and 10 and 100)
 *
 * @author Administrator
 */
public class CustomValueWithAggregatingState {

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

        keyedStream.flatMap(new AggregatingStateImp()).print();

        env.execute("CustomValueWithAggregatingState");
    }
}

/**
 * @author Administrator
 */
class AggregatingStateImp extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, String>> {

    /**
     * 一个key的state
     * Long:value输入的类型
     * String:value输出的类型
     */
    private AggregatingState<Long, String> totalStr;

    /**
     * Long:输入类型
     * String:accumulator类型
     * String:输出类型
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        //注册状态
        //IN, ACC(accumulator) 辅助变量, OUT
        //这地方的数据类型有点特殊
        AggregatingStateDescriptor<Long, String, String> descriptor = new AggregatingStateDescriptor<Long, String, String>(
                //状态的名字
                "totalStr",
                new AggregateFunction<Long, String, String>() {

                    //创建accumulator累加变量
                    //String str="Contains:"
                    @Override
                    public String createAccumulator() {
                        return "Contains：";
                    }

                    @Override
                    public String add(Long value, String accumulator) {
                        if ("Contains：".equals(accumulator)) {
                            return accumulator + value;
                        }
                        return accumulator + " and " + value;
                    }

                    //获取accumulator
                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    //合并不同task里面的数据，所以就我们这个需求而言
                    //这个方法是没有用的
                    //因为我们就只有一个task
                    //因为我们的操作都是在keyup之后的数据,也就是说已经经过shuffle操作后
                    //相同key的数据都在同一个task中

                    //如果在shuffle之前的操作,这个merge就可以合并不同task中的数据
                    @Override
                    public String merge(String a, String b) {
                        return a + " and " + b;
                    }
                    //状态存储的输出的数据类型
                    //这里就是contains:xxx的字符串
                }, String.class);

        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> element, Collector<Tuple2<String, String>> out) throws Exception {

        //更改状态值
        totalStr.add(element.f1);
        //输出到下游
        out.collect(Tuple2.of(element.f0, totalStr.get()));
    }
}
