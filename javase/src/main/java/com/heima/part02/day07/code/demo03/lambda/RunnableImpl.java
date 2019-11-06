package com.heima.part02.day07.code.demo03.lambda;
/*
    创建Runnable接口的实现类,重写run方法,设置线程任务
 */
public class RunnableImpl implements Runnable{
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+" 新线程创建了");
    }
}
