package com.peng.source.parallelism;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author Administrator
 */
public class MyParallelismSource implements ParallelSourceFunction<Long> {

    private long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning) {
            //输出到下游
            sct.collect(number);
            //自增1
            number++;
            //每秒生成一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
