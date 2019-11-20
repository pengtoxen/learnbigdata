package com.peng.namenode_demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

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
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        Task task = new Task(fs);
        //开启1000个线程跑任务
        for (int i = 0; i < 1000; i++) {
            executorService.submit(task);
        }
        //运行完毕,关闭线程池
        executorService.shutdown();
    }
}
