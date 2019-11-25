package com.peng.report.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * IN,输入的数据类型
 * OUT,输出的数据类型
 * KEY,在flink里面这儿其实就是分组的字段，大家永远看到的是一个tuple字段
 * 只不过，如果你的分组的字段是有一个，那么这个tuple里面就只会有一个字段
 * 如果说你的分组的字段有多个，那么这个里面就会有多个字段
 * W extends Window
 *
 * @author Administrator
 */
public class MySumFunction implements WindowFunction<Tuple3<Long, String, String>, Tuple4<String, String, String, Long>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow,
                      Iterable<Tuple3<Long, String, String>> input,
                      Collector<Tuple4<String, String, String, Long>> out) {
        //获取分组字段信息
        //视频类型
        String type = tuple.getField(0).toString();
        //地区
        String area = tuple.getField(1).toString();

        java.util.Iterator<Tuple3<Long, String, String>> iterator = input.iterator();
        long count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(new Date(timeWindow.getEnd()));

        //time:
        Tuple4<String, String, String, Long> result = new Tuple4<String, String, String, Long>(time, type, area, count);

        //输出到下游
        out.collect(result);
    }
}
