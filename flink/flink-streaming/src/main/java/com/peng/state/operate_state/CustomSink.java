package com.peng.state.operate_state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 输入数据类型:Tuple2<String, Integer>
 *
 * 恢复状态数据到ListState内存bufferElements
 *
 * @author Administrator
 */
public class CustomSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    /**
     * 这个数据是存在内存里面的，我们的程序重启，数据就会丢失了
     */
    private List<Tuple2<String, Integer>> bufferElements;

    /**
     * 不同于基于key的keyedState,每条数据都可以存储
     * 实现程序比较安全,程序重启后也不丢失数据
     */
    private ListState<Tuple2<String, Integer>> checkpointState;

    /**
     * 定义一个阈值
     * 每隔几条数据打印一次
     */
    private int threshold;

    public CustomSink(int threshold) {
        this.threshold = threshold;
        bufferElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {

        //来一条数据,存储到bufferElements
        bufferElements.add(value);
        //bufferElements等于设定值,打印出来
        if (bufferElements.size() == threshold) {
            System.out.println("数据：" + bufferElements);
            bufferElements.clear();
        }
    }

    /**
     * 定时地往状态中存数据
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //存数据之前clear
        checkpointState.clear();
        for (Tuple2<String, Integer> ele : bufferElements) {
            //将当前数据存入
            checkpointState.add(ele);
        }
    }

    /**
     * 重启的时候初始化状态
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                "buffer",
                //这里参数和keyedState不一样
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })
        );

        //初始化
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        //如果任务重启了,可以通过isRestored()判断
        if (context.isRestored()) {
            for (Tuple2<String, Integer> ele : checkpointState.get()) {
                bufferElements.add(ele);
            }
        }
    }
}
