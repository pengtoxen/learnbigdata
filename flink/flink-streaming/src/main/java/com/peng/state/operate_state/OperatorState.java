package com.peng.state.operate_state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求:
 * 每两条数据打印一次
 * <p>
 * 输入数据:
 * spark,3
 * hadoop,5
 * hadoop,7
 * flink,4
 * <p>
 * 打印结果:
 * [spark,3  hadoop,5]
 * [hadoop,7  flink,4]
 *
 * operatorState跟key没关系
 *
 * @author Administrator
 */
public class OperatorState {

    public static void main(String[] args) throws Exception {

        //准备上下文环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //测试数据
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource =
                env.fromElements(
                        Tuple2.of("Spark", 3),
                        Tuple2.of("Hadoop", 5),
                        Tuple2.of("Hadoop", 7),
                        Tuple2.of("flink", 4)
                );

        //自定义sink
        dataStreamSource.addSink(new CustomSink(2)).setParallelism(1);

        env.execute("OperatorState");
    }
}
