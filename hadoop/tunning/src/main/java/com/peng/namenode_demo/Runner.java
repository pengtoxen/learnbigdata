package com.peng.namenode_demo;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    /**
     * 线程池的核心线程数量
     *
     * CPU 密集型任务(N+1)： 这种任务消耗的主要是 CPU 资源，可以将线程数设置为 N（CPU 核心数）+1，
     * 比 CPU 核心数多出来的一个线程是为了防止线程偶发的缺页中断，或者其它原因导致的任务暂停而带来的影响。
     * 一旦任务暂停，CPU 就会处于空闲状态，而在这种情况下多出来的一个线程就可以充分利用 CPU 的空闲时间。
     *
     * I/O 密集型任务(2N)： 这种任务应用起来，系统会用大部分的时间来处理 I/O 交互，而线程在处理 I/O
     * 的时间段内不会占用 CPU 来处理，这时就可以将 CPU 交出给其它线程使用。因此在 I/O 密集型任务的应用中，
     * 我们可以多配置一些线程，具体的计算方法是 2N。
     */
    private static final int CORE_POOL_SIZE = 3;
    /**
     * 如果队列未满就存放线程,满了的话,线程池扩大为max数量
     */
    private static final int MAX_POOL_SIZE = 6;
    /**
     * 存放线程的队列大小
     * 有界队列和无界队列
     */
    private static final int QUEUE_CAPACITY = 100;
    /**
     * 线程池中有11个线程,corePoolSize设置的线程是10个
     * 那么有1个线程是超出的,而且当这个线程处于空闲状态时
     * 超过keepAliveTime的时间,还没有任务,那么就释放掉
     */
    private static final Long KEEP_ALIVE_TIME = 1L;

    public static void main(String[] args) {

        FSEdit fs = new FSEdit();

        //使用阿里巴巴推荐的创建线程池的方式
        //通过ThreadPoolExecutor构造函数自定义参数创建
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                new ThreadPoolExecutor.CallerRunsPolicy());
        Task task = new Task(fs);

        //开启1000个线程跑任务
        for (int i = 0; i < 1000; i++) {
            executorService.submit(task);
        }
        //运行完毕,关闭线程池
        executorService.shutdown();
    }
}
