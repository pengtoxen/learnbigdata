package com.peng.juc;

import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConditionDemo {

    /**
     * 测试任务
     *
     * @throws InterruptedException
     */
    @Test
    public void runTask() throws InterruptedException {
        TaskQueue taskQueue = new TaskQueue();
        WorkThread worker = new WorkThread(taskQueue);
        //启动线程
        worker.start();
        //add task
        taskQueue.addTask("Bob");
        Thread.sleep(2000);
        taskQueue.addTask("Tom");
        Thread.sleep(2000);
        taskQueue.addTask("Jenny");
        Thread.sleep(2000);
        worker.interrupt();
        //加入到当前线程
        worker.join();
        System.out.println("done");
    }
}

/**
 * 任务队列
 */
class TaskQueue {

    private final Queue<String> queue = new LinkedList();
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    String getTask() throws InterruptedException {
        lock.lock();
        try {
            while (this.queue.isEmpty()) {
                //当前线程等待
                notEmpty.await();
            }
            return queue.remove();
        } finally {
            lock.unlock();
        }
    }

    void addTask(String name) {
        lock.lock();
        try {
            this.queue.add(name);
            //唤醒其他等待的线程
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

/**
 * 工作线程
 */
class WorkThread extends Thread {

    private TaskQueue taskQueue;

    WorkThread(TaskQueue taskQueue) {
        this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            String name;
            try {
                name = taskQueue.getTask();
            } catch (InterruptedException e) {
                break;
            }
            String result = "Hello, " + name + "!";
            System.out.println(result);
        }
    }
}