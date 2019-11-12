package com.peng.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        //步骤一：获取执行环境，程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //步骤二：获取数据源
        DataSource<String> dataSet = env.readTextFile("flink-batch/testdata/rawdata/wordcount/data.text");
        //步骤三：数据处理
        AggregateOperator<Tuple2<String, Integer>> result = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word, 1));
                }

            }
        }).groupBy(0)
                .sum(1);

        //步骤四：数据结果处理
        result.writeAsText("flink-batch/testdata/output/wordcount");

        //步骤五：启动程序
        env.execute("batch execute ......");
    }
}
