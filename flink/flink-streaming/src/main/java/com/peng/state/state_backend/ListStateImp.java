package com.peng.state.state_backend;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Administrator
 */
public class ListStateImp extends RichFlatMapFunction<Tuple2<String, Long>, String> implements CheckpointedFunction {

    /**
     * 我们的数据源那儿的每个key都会有自己的一个ValueState
     * Tuple2<String, Long>
     * String:key键
     * Long:value值
     * <p>
     * 不同key的数据是不会混乱的，因为每个key都有自己的state
     */
    private ListState<Tuple2<String, Long>> checkpointState;

    /**
     * 需要缓冲在内存中的数据
     */
    private List<Tuple2<String, Long>> bufferedData;

    /**
     * 需要监控的阈值
     */
    private Long threshold;

    ListStateImp(Long threshold) {
        this.threshold = threshold;
        this.bufferedData = new ArrayList<>();
    }

    /**
     * @param element
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<String, Long> element, Collector<String> out) throws Exception {

        //往集合添加数据
        bufferedData.add(element);

        //数据等于threshold,拼接字符串
        if (bufferedData.size() == threshold) {
            String str = "";
            for (Tuple2<String, Long> ele : bufferedData) {
                str += ele.toString();
                str += " ";
            }
            //输出到下游
            out.collect(str);
            //计算完成后,清空bufferedData
            bufferedData.clear();
        }
    }

    /**
     * 更新数据到状态中
     * 每隔一段时间执行一次操作
     *
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //在进行快照时，将数据存储到checkPointedState
        checkpointState.clear();
        for (Tuple2<String, Long> element : bufferedData) {
            //将当前数据存入
            checkpointState.add(element);
        }
    }

    /**
     * 初始化/重启的时候,初始化状态,从状态中获取数据
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //注意这里获取的是OperatorStateStore
        checkpointState = context.getOperatorStateStore().
                getListState(new ListStateDescriptor<>("double-print",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        })));
        //如果发生重启，则需要从快照中将状态进行恢复
        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkpointState.get()) {
                bufferedData.add(element);
            }
        }
    }
}