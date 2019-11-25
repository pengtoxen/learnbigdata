package com.peng.report.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 自定义watermark
 * 水位的理解
 * =====================================
 * 备注         key     event_time  currentMaxTimestamp     currentWatermark        window_start_time    window_end_time
 * 第一条数据    000001   19:34:30        19:34:30                19:34:20
 * 第二条数据    000002   19:34:43        19:34:43                19:34:33               [19:34:30            19:34:33)
 * 第二条数据过来的时候,watermark的时间是19:34:33,在[19:34:30 , 19:34:33)窗口中,key=000001的数据的事件时间刚好落在这个区间里,满足条件,进行处理
 * <p>
 * 第三条数据    000003   19:34:45        19:34:45                19:34:35
 * 第四条数据    000004   19:34:33        19:34:45                19:34:35
 * 第五条数据    000005   19:34:34        19:34:46                19:34:36               [19:34:33            19:34:36)
 * 第五条数据过来的时候,watermark的时间是19:34:35,在[19:34:33 , 19:34:36)窗口中,key=000004,key=000005的数据的事件时间刚好落在这个区间里,满足条件,进行处理
 * <p>
 * 从上面的结果可以看出,保证了有序
 * <p>
 * 结论
 * 1.currentWatermark = window_end_time
 * 2.在 [window_start_time, window_end_time) 区间中有数据存在，注意是左闭右开的区间，而且是以event time来计算的
 *
 * @author Administrator
 */
public class MyWaterMark implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {

    /**
     * 当前最大时间
     * 即目前事件时间的最大值
     */
    private long currentMaxTimestamp = 0L;

    /**
     * 允许乱序时间10秒
     */
    private final long disorderMaxTime = 10000L;

    /**
     * 获取当前的watermark时间
     *
     * @return
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - disorderMaxTime);
    }

    /**
     * 提取element中的事件时间
     * 并更新当前最大时间
     *
     * @param element
     * @param l
     * @return
     */
    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long l) {

        //第一个字段f0是事件发生的时间
        Long timeStamp = element.f0;
        this.updateCurrentMaxTimestamp(timeStamp);
        return timeStamp;
    }

    /**
     * 更新当前最大时间
     * @param timeStamp
     */
    private void updateCurrentMaxTimestamp(Long timeStamp) {
        currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
    }
}
