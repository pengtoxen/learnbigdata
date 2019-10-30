package com.peng.date;

import org.junit.Test;

import java.util.Date;

class Base {
    public static void main(String[] args) {
    }

    /**
     * public Date()：分配Date对象并初始化此对象，以表示分配它的时间（精确到毫秒）。
     * public Date(long date)：分配Date对象并初始化此对象，以表示自从标准基准时间（称为“历元（epoch）”，
     * 即1970年1月1日00:00:00 GMT）以来的指定毫秒数。
     * public long getTime() 把日期对象转换成对应的时间毫秒值。
     */
    @Test
    public void initDate() {
        // 创建日期对象，把当前的时间
        System.out.println(new Date()); // Tue Jan 16 14:37:35 CST 2018
        // 创建日期对象，把当前的毫秒值转成日期对象
        System.out.println(new Date(0L)); // Thu Jan 01 08:00:00 CST 1970
        // 把日期对象转换成对应的时间毫秒值
        (new Date()).getTime();
    }
}