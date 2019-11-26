package com.peng.window.global;

import com.peng.window.CustomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * 单词每出现三次统计一次
 *
 * @author Administrator
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<String> dataStream = env.addSource(new CustomSource());

        //flatMap
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });


        stream.keyBy(0)
                //global窗口,把整个流当做窗口
                .window(GlobalWindows.create())
                //触发器,当数据=3个的时候就触发计算,计算一次
                .trigger(CountTrigger.of(3))
                .sum(1)
                .print();

        env.execute("WordCount");
    }
}
