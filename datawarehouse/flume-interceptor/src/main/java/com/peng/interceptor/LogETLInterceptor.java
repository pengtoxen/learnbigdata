package com.peng.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * ETL拦截器主要用于过来时间戳不合法和json数据不完整的日志
 *
 * @author Administrator
 */

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    /**
     * 拦截数据方法
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {

        // 1 获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        // 2 判断数据类型并向Header中赋值
        if (log.contains("start")) {
            //启动日志规则判断---数据清洗
            if (LogUtils.validateStart(log)) {
                return event;
            }
        } else {
            //事件日志规则判断---数据清洗
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }

        // 3 返回校验结果
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event interceptETL = intercept(event);

            if (interceptETL != null) {
                interceptors.add(interceptETL);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}