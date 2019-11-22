package com.peng.state.keyed_state.order_demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * OrderInfo1 第一个流的数据类型
 * OrderInfo2 第二个流的数据类型
 * Tuple2<OrderInfo1, OrderInfo2> 输出的数据类型
 *
 * @author Administrator
 */
public class EnrichmentFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {

    /**
     * 第一个流的状态值
     */
    private ValueState<OrderInfo1> orderInfo1ValueState;

    /**
     * 第二个流的状态值
     */
    private ValueState<OrderInfo2> orderInfo2ValueState;

    @Override
    public void open(Configuration parameters) throws Exception {

        //注册ValueState
        ValueStateDescriptor<OrderInfo1> descriptor1 = new ValueStateDescriptor<>(
                "info1",
                OrderInfo1.class
        );

        //注册ValueState
        ValueStateDescriptor<OrderInfo2> descriptor2 = new ValueStateDescriptor<>(
                "info2",
                OrderInfo2.class
        );

        orderInfo1ValueState = getRuntimeContext().getState(descriptor1);
        orderInfo2ValueState = getRuntimeContext().getState(descriptor2);
    }

    /**
     * 针对第一个流的flatMap的key处理逻辑
     * 相同的key的数据才会进来这里
     * 这个方法要是被运行，那说明第一个流肯定是来数据了
     *
     * 1.第一个流的数据存起来
     * 2.看看第二个流的数据有没有到,到了就组合,组合后输出到下游
     *
     * @param orderInfo1
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap1(OrderInfo1 orderInfo1, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
        //看看第二个流中有没有数据,拿到第二个流中的数据
        OrderInfo2 value2 = orderInfo2ValueState.value();
        //value2!=null说明第二个流中已经有数据
        if (value2 != null) {
            //代码运行到这里,说明将第二个流中的状态数据已经被使用,清空
            //以便接收后面的数据
            orderInfo2ValueState.clear();
            //往下游输出数据
            //合并两个数据
            //在这里,就是key相同的两个流数据这到一块了
            out.collect(Tuple2.of(orderInfo1, value2));
        } else {
            //第二个流还没有来,把第一个流的数据存起来
            orderInfo1ValueState.update(orderInfo1);
        }
    }

    /**
     * 针对第二个流的flatMap的key处理逻辑
     * 相同的key的数据才会进来这里
     * 这个方法要是被运行，那说明第二个流肯定是来数据了
     *
     * @param orderInfo2
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap2(OrderInfo2 orderInfo2, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
        OrderInfo1 value1 = orderInfo1ValueState.value();
        if (value1 != null) {
            orderInfo1ValueState.clear();
            out.collect(Tuple2.of(value1, orderInfo2));
        } else {
            orderInfo2ValueState.update(orderInfo2);
        }
    }
}
