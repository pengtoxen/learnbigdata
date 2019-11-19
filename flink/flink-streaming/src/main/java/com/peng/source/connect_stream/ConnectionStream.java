package com.peng.source.connect_stream;

import com.peng.source.no_parallelism.MyNoParallelismSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 *
 * @author Administrator
 */
public class ConnectionStream {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        //注意：针对此source，并行度只能设置为1
        DataStreamSource<Long> text1 = env.addSource(new MyNoParallelismSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNoParallelismSource()).setParallelism(1);

        SingleOutputStreamOperator<String> formatText2 = text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        //合并两个流
        ConnectedStreams<Long, String> connectStream = text1.connect(formatText2);

        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {

            @Override
            public Object map1(Long value) throws Exception {
                //System.out.println("业务逻辑的处理");
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        //打印结果
        result.print().setParallelism(1);
        String jobName = ConnectionStream.class.getSimpleName();
        env.execute(jobName);
    }
}
