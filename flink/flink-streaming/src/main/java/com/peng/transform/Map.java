package com.peng.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据源：1 2 3 4 5.....源源不断过来
 * 通过map打印一下接收到数据
 * 通过filter过滤一下数据，我们只需要偶数
 */
public class Map {
    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义source
        DataStreamSource<Long> numberStream = env.addSource(new MyNoParallelismSource()).setParallelism(1);

        //map操作
        SingleOutputStreamOperator<Long> dataStream = numberStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接受到了数据：" + value);
                return value;
            }
        });

        //filter操作
        SingleOutputStreamOperator<Long> filterDataStream = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long number) throws Exception {
                return number % 2 == 0;
            }
        });

        //打印输出
        filterDataStream.print().setParallelism(1);

        env.execute("Map");
    }
}