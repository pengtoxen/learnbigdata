package com.peng.juc;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 特点
 * 1.只允许一个线程写入(其他线程不能写入和读取)
 * 2.没有写入时,多个线程允许同时读(提高性能)
 * <p>
 * 总结
 * 1.ReadWriteLock只允许一个线程写入
 * 2.ReadWriteLock允许多个线程同时读取
 * 3.必须使用try...finally保证正确获取和释放锁
 * 4.ReadWriteLock适合读多写少的场景
 *
 * @author Administrator
 */
public class ReadWriteLockDemo {
    final static int LOOP = 100;

    public static void main(String[] args) throws InterruptedException {
        Counter2 counter = new Counter2();
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


class Counter2 {
    //向上转型
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock rlock = lock.readLock();
    private Lock wlock = lock.writeLock();

    int value = 0;

    public void add(int m) {
        wlock.lock();
        try {
            this.value += m;
        } finally {
            wlock.unlock();
        }
    }

    public void dec(int m) {
        wlock.lock();
        try {
            this.value -= m;
        } finally {
            wlock.unlock();
        }
    }

    public int get() {
        rlock.lock();
        try {
            return this.value;
        } finally {
            rlock.unlock();
        }
    }
}
