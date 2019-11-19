package com.peng.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * 根据指定的字段rightJoin两个数据源
 *
 * @author Administrator
 */
public class RightJoin {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //第一组数据
        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "李伟"));
        data1.add(new Tuple2<>(2, "唐恩明"));
        data1.add(new Tuple2<>(3, "唐剑斌"));

        //第二组数据
        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(4, "guangzhou"));

        DataSource<Tuple2<Integer, String>> dataSet1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSet2 = env.fromCollection(data2);

        //dataset1在前面,dataset2在后面
        //所以where的参数是dataset1的id
        //equalTo的参数是dataset2的id
        //with里是join的逻辑

        //输入是<Tuple2<Integer, String>,<Tuple2<Integer, String>
        //输出是Tuple3<Integer, String, String>
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> result = dataSet1
                .rightOuterJoin(dataSet2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> d1, Tuple2<Integer, String> d2) throws Exception {
                        if (d1 == null) {
                            return new Tuple3<>(d2.f0, "null", d2.f1);
                        } else {
                            return new Tuple3<>(d2.f0, d1.f1, d2.f1);
                        }
                    }
                });

        //输出结果
        result.print();
    }
}
