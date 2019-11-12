package com.peng.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * flink窗口计算,每1秒统计2秒内的数据
 * 自定义输出格式
 * 抽离业务逻辑
 *
 * @author Administrator
 */
public class WordCountTaskDriver {
    public static void main(String[] args) throws Exception {

        //步骤一：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //步骤二：获取数据源
        //这里监听本地端口8888
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);

        //步骤三：执行逻辑操作
        SingleOutputStreamOperator<WordAndCount> result = dataStream
                .flatMap(new SplitWordTask())
                .keyBy("word")
                //每隔1秒计算最近2秒的数据
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
        //步骤四：结果打印
        result.print();
        //步骤五：任务启动
        env.execute("WordCount Test");
    }

    /**
     * 把获取到的数据分割成为一个一个的单词
     * 公司里面开发一般都是这样子的。业务逻辑基本都是在里面写的
     */
    public static class SplitWordTask implements FlatMapFunction<String, WordAndCount> {
        @Override
        public void flatMap(String line, Collector<WordAndCount> out) throws Exception {
            String[] fields = line.split(",");
            for (String word : fields) {
                out.collect(new WordAndCount(word, 1));
            }
        }
    }

    /**
     * flatMap自定义的输出格式
     */
    public static class WordAndCount {

        String word;
        int count;

        public WordAndCount() {

        }

        WordAndCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}