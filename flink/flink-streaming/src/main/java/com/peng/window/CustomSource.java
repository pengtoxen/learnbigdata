package com.peng.window;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

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
public class CustomSource implements SourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sct) throws Exception {
        String[] words = {"hadoop", "flink", "hive", "spark"};
        Random random = new Random();
        while (isRunning) {
            //组装数据
            int i = random.nextInt(words.length);
            int j = random.nextInt(words.length);
            String line = words[i] + "," + words[j];
            //每秒生成一条数据,输出到下游
            sct.collect(line);
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