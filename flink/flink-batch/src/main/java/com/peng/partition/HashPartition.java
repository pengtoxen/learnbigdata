package com.peng.partition;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * HashPartition
 * <p>
 * RangePartition
 */
public class HashPartition {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //设置2个并行度
        env.setParallelism(2);

        //准备测试数据
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1, "hello1"));
        data.add(new Tuple2<>(2, "hello2"));
        data.add(new Tuple2<>(2, "hello3"));
        data.add(new Tuple2<>(3, "hello4"));
        data.add(new Tuple2<>(3, "hello5"));
        data.add(new Tuple2<>(3, "hello6"));
        data.add(new Tuple2<>(4, "hello7"));
        data.add(new Tuple2<>(4, "hello8"));
        data.add(new Tuple2<>(4, "hello9"));
        data.add(new Tuple2<>(4, "hello10"));
        data.add(new Tuple2<>(5, "hello11"));
        data.add(new Tuple2<>(5, "hello12"));
        data.add(new Tuple2<>(5, "hello13"));
        data.add(new Tuple2<>(5, "hello14"));
        data.add(new Tuple2<>(5, "hello15"));
        data.add(new Tuple2<>(6, "hello16"));
        data.add(new Tuple2<>(6, "hello17"));
        data.add(new Tuple2<>(6, "hello18"));
        data.add(new Tuple2<>(6, "hello19"));
        data.add(new Tuple2<>(6, "hello20"));
        data.add(new Tuple2<>(6, "hello21"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
        /*
         * 数据倾斜，如何缓解一下数据倾斜
         * 但是如果你的一个key确实很多，数据量很大。
         * Spark里面已经讲了
         * 可以加一些随机前缀等等去处理
         */
        //根据第一个字段做hash计算值计算分区
        text.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {

            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()) {
                    Tuple2<Integer, String> next = it.next();
                    System.out.println("当前线程id：" + Thread.currentThread().getId() + "," + next);
                }
            }
        }).print();
        /*
         * 当前线程id：57,(1,hello1)
         * 当前线程id：57,(5,hello11)
         * 当前线程id：57,(5,hello12)
         * 当前线程id：57,(5,hello13)
         * 当前线程id：57,(5,hello14)
         * 当前线程id：57,(5,hello15)
         * 当前线程id：57,(6,hello16)
         * 当前线程id：57,(6,hello17)
         * 当前线程id：57,(6,hello18)
         * 当前线程id：57,(6,hello19)
         * 当前线程id：57,(6,hello20)
         * 当前线程id：57,(6,hello21)
         * 当前线程id：59,(2,hello2)
         * 当前线程id：59,(2,hello3)
         * 当前线程id：59,(3,hello4)
         * 当前线程id：59,(3,hello5)
         * 当前线程id：59,(3,hello6)
         * 当前线程id：59,(4,hello7)
         * 当前线程id：59,(4,hello8)
         * 当前线程id：59,(4,hello9)
         * 当前线程id：59,(4,hello10)
         * 从结果可以看到,因为设置了2个并行度,也就是有2个task在运行
         * 根据第一个字段的hash值,分给了不同的task去处理
         * 所以线程id只有两个57和59,分别处理不同的数据
         */
    }
}