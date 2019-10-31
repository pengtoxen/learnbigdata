package com.peng.juc;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 可重入锁
 * 1.一个线程可以多次获得同一个锁
 * 2.lock()方法可获得锁
 * 3.tryLock()方法可尝试获取锁并指定超时
 *
 * 总结
 * 1.ReentrantLock可以替代synchronized
 * 2.ReentrantLock获取锁更安全
 * 3.必须使用try...finally保证正确获取和释放锁
 *
 * @author Administrator
 */
public class ReentrantLockDemo {
    final static int LOOP = 100;

    public static void main(String[] args) throws InterruptedException {
        Counter counter = new Counter();
        Thread t1 = new Thread() {
            @Override
            public void run() {
                //多个线程add
                for (int i = 0; i < LOOP; i++) {
                    counter.add(1);
                }
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                //多个线程dec
                for (int i = 0; i < LOOP; i++) {
                    counter.dec(1);
                }
            }
        };
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        System.out.println(counter.get());
    }
}


class Counter {
    //向上转型
    private Lock lock = new ReentrantLock();
    int value = 0;

    public void add(int m) {
        lock.lock();
        try {
            this.value += m;
        } finally {
            lock.unlock();
        }
    }

    public void dec(int m) {
        lock.lock();
        try {
            this.value -= m;
        } finally {
            lock.unlock();
        }
    }

    public int get() {
        lock.lock();
        try {
            return this.value;
        } finally {
            lock.unlock();
        }
    }
}
