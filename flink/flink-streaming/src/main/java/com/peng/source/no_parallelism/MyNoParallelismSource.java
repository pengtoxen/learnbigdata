package com.peng.source.no_parallelism;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 实现接口
 * long类型代表的是输出的数据类型
 * <p>
 * String
 * Tuple2
 * Student
 *
 * @author Administrator
 */
public class MyNoParallelismSource implements SourceFunction<Long> {

    private long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning) {
            //每秒生成一条数据,输出到下游
            sct.collect(number);
            //自增1
            number++;
            //睡眠1秒
            Thread.sleep(1000);
        }
    }

    /**
     * 停止任务的时候会运行这个方法。
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}