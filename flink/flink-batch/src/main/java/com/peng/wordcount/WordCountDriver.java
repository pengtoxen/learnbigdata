package com.peng.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Administrator
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        //步骤一：获取执行环境，程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //步骤二：获取数据源
        DataSource<String> dataSet = env.readTextFile("flink-batch/testdata/rawdata/wordcount/data.text");
        //步骤三：数据处理
        FlatMapOperator<String,Tuple2<String, Integer>> wordAndOneDataSet = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    //Tuple2的两种写法
                    out.collect(Tuple2.of(word, 1));
                    //out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        //按key聚合
        AggregateOperator<Tuple2<String, Integer>> result = wordAndOneDataSet.groupBy(0).sum(1);

        //步骤四：数据结果处理
        result.writeAsText("flink-batch/testdata/output/wordcount");

        //步骤五：启动程序
        env.execute("word count");
    }
}
