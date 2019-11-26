package com.peng.window.session;

import com.peng.window.CustomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 5秒过去以后，该单词不出现就打印出来该单词
 *
 * @author Administrator
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<String> dataStream = env.addSource(new CustomSource());

        //逻辑处理
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
                //session窗口
                //数据不断的发送,当已经过去5秒后,还没有收到下一个数据,那么之前的数据算一个窗口的数据
                //后面再有数据过来,是进入另外一个窗口
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute("WordCount");
    }
}
