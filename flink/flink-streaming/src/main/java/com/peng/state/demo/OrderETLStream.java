package com.peng.state.demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Administrator
 */
public class OrderETLStream {

    public static void main(String[] args) throws Exception {

        //获取环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义数据源,从文件中读取,获取两个数据流
        DataStreamSource<String> info1Stream = env.addSource(new FileSource(Constants.ORDER_INFO1_PATH));
        DataStreamSource<String> info2Stream = env.addSource(new FileSource(Constants.ORDER_INFO2_PATH));

        //面向对象的思想处理OrderInfo1流
        SingleOutputStreamOperator<OrderInfo1> orderInfo1Stream = info1Stream.map(line -> OrderInfo1.line2Info1(line));

        //面向对象的思想处理OrderInfo2流
        SingleOutputStreamOperator<OrderInfo2> orderInfo2Stream = info2Stream.map(line -> OrderInfo2.line2Info2(line));

        //keyBy操作,将OrderInfo1流的数据按订单号分组
        KeyedStream<OrderInfo1, Long> keyByInfo1 = orderInfo1Stream.keyBy(orderInfo1 -> orderInfo1.getOrderId());

        //keyBy操作,将OrderInfo2流的数据按订单号分组
        KeyedStream<OrderInfo2, Long> keyByInfo2 = orderInfo2Stream.keyBy(orderInfo2 -> orderInfo2.getOrderId());

        //拼接两个流
        //两个流有先后顺序,不能用join
        keyByInfo1.connect(keyByInfo2)
                .flatMap(new EnrichmentFunction())
                .print();

        env.execute("OrderETLStream");
    }
}
