package com.peng.map_partition;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * flink的mapPartition和spark的一样
 * 都是对一个分区里的数据进行操作
 * @author Administrator
 */
public class MapPartition {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> data = new ArrayList<>();
        data.add("you,jump");
        data.add("i,jump");
        DataSource<String> dataSet = env.fromCollection(data);

        /*
         * flink和spark类似,都有map和mapPartition
         * spark:map mapPartition
         * flink:map mapPartition
         */
        MapPartitionOperator<String, String> wordDataSet = dataSet.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                Iterator<String> it = iterable.iterator();
                while (it.hasNext()) {
                    String line = it.next();
                    String[] fields = line.split(",");
                    for (String word : fields) {
                        collector.collect(word);
                    }
                }
            }
        });

        wordDataSet.print();
    }
}
