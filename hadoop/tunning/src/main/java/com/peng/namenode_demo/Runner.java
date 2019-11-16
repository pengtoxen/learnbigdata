package com.peng.namenode_demo;

/**
 * HDFS管理元数据
 * 超高并发刷写磁盘
 * 每秒2000并发
 *
 * @author Administrator
 * <p>
 * 这段代码是模仿hadoop的源码的写的
 */
public class Runner {

    public static void main(String[] args) {
        FSEdit fs = new FSEdit();
        //开启1000个线程跑任务
        for (int i = 0; i < 1000; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    //每个线程写1000条数据
                    for (int j = 0; j < 1000; j++) {
                        fs.logEdit("元数据");
                    }
                }
            }).start();
        }
    }
}
