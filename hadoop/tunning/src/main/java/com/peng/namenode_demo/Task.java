package com.peng.namenode_demo;

/**
 * 写元数据任务
 * @author Administrator
 */
public class Task implements Runnable {

    private FSEdit task;

    Task(FSEdit fs) {
        task = fs;
    }

    @Override
    public void run() {
        //每个线程写1000条数据
        for (int j = 0; j < 1000; j++) {
            task.logEdit("元数据");
        }
    }
}
