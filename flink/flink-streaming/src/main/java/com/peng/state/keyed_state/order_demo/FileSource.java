package com.peng.state.keyed_state.order_demo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源
 * 从文件中获取数据
 *
 * @author Administrator
 */
public class FileSource implements SourceFunction<String> {

    private String filePath;
    private BufferedReader reader;
    private Random random = new Random();

    public FileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sct) throws Exception {
        //获取流
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line = null;
        while ((line = reader.readLine()) != null) {
            //模拟数据源源不断的感觉，所以我让线程sleep一下
            //0-500ms随机sleep
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            //往下游发送数据
            sct.collect(line);
        }
    }

    @Override
    public void cancel() {
        try {
            if (reader == null) {
                //取消的时候,关闭流
                reader.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
