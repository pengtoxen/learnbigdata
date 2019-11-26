package com.peng.time.process;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每隔5秒计算最近10秒单词出现的次数
 *
 * @author Administrator
 */
public class TimeWindowProcess {
    public static void main(String[] args) throws Exception {

        //上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);

        //逻辑处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0)
                //每隔5秒计算最近10秒单词出现的次数
                .timeWindow(Time.seconds(10), Time.seconds(5))
                //自定义处理逻辑实现sum函数功能
                .process(new SumProcessWindowFunction());

        result.print().setParallelism(1);

        env.execute("TimeWindow");
    }

    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型（在Flink里面，String用Tuple表示）
     * W：Window的数据类型
     */
    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        /**
         * 当一个window触发计算的时候会调用这个方法
         *
         * @param tuple    key
         * @param context  operator的上下文
         * @param elements 指定window的所有元素
         * @param out      用户输出
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) {

            //System.out.println("当天系统的时间："+dateFormat.format(System.currentTimeMillis()));
            //System.out.println("Window的处理时间："+dateFormat.format(context.currentProcessingTime()));
            //System.out.println("Window的开始时间："+dateFormat.format(context.window().getStart()));
            //System.out.println("Window的结束时间："+dateFormat.format(context.window().getEnd()));

            int sum = 0;
            for (Tuple2<String, Integer> ele : elements) {
                sum += 1;
            }
            //输出单词出现的次数
            out.collect(Tuple2.of(tuple.getField(0), sum));
        }
    }
}
