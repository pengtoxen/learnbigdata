package com.peng.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink窗口计算,每1秒统计2秒内的数据
 *
 * @author Administrator
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        ////flink提供的工具类，获取传递的参数
        //ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ////主机名
        //String hostname = parameterTool.get("hostname");
        ////端口号
        //int port = parameterTool.getInt("port");

        //步骤一：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //步骤二：获取数据源
        //这里监听本地端口8888
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        //步骤三：执行逻辑操作
        /*
         * hadoop,hadoop,hive
         *
         * hadoop,1
         * hadoop,1
         * hive,1
         *
         * hadoop,{1,1}
         * hive,{1}
         *
         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0)
                //每隔1秒计算最近2秒的数据
                //.timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);
        //步骤四：结果打印
        result.print();
        //步骤五：任务启动
        env.execute("WordCount Test");
    }
}
