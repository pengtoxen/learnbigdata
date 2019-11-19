package com.peng.source.parallelism;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 */
public class StreamingWithMyParallelismSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据源,设置并行度2
        //设置并行度2就是有2个数据源并行的发送数据
        DataStreamSource<Long> numberStream = env.addSource(new MyParallelismSource()).setParallelism(3);

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

        //打印输出,设置并行度1
        //filterDataStream.print().setParallelism(1);
        env.execute("StreamingWithMyParallelismSource");
    }
}
