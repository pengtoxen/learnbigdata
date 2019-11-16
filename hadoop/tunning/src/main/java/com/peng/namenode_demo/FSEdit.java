package com.peng.namenode_demo;

/**
 * 高并发写元数据
 */
class FSEdit {

    private long taxId = 0L;
    private DoubleBuffer doubleBuffer = new DoubleBuffer();
    //每个线程自己拥有的副本
    private ThreadLocal<Long> threadLocal = new ThreadLocal<Long>();
    //是否有线程正在把数据同步到磁盘上
    private boolean isSyncRunning = false;
    //正在刷写数据到磁盘的内存块里面最大的一个事务ID号
    private long maxTaxId = 0L;
    private boolean isWait = false;

    /**
     * 写元数据日志的核心方法
     *
     * @param log
     */
    void logEdit(String log) {
        //能支持超高并发
        //线程1 maxTaxId=1
        //线程2 maxTaxId=2
        //线程3 maxTaxId=3
        //分段加锁
        //线程1进来的抢到锁,线程2和线程3阻塞
        synchronized (this) {
            taxId++;
            threadLocal.set(taxId);
            EditLog editLog = new EditLog(taxId, log);
            //往内存里面写数据
            doubleBuffer.write(editLog);
        }
        //释放锁
        //线程1释放锁,线程2抢占锁,但是不会影响到这里,因为这里没有加锁
        //代码运行到这的时候,可能线程2和线程也运行到这里了
        //所以currentBuffer内存里面已经有3条数据
        //重新加锁
        logFlush();
    }

    /**
     * 刷新数据到磁盘
     */
    private void logFlush() {
        //重新加锁
        synchronized (this) {
            //线程1进来的时候,isSyncRunning=false
            if (isSyncRunning) {
                //isSyncRunning=true,说明有线程在写数据,当前线程进入这里
                //获取当前线程的是事务ID
                long localTaxId = threadLocal.get();
                //对比当前线程的事务ID和正在同步数据的内存里面事务ID最大的ID的大小
                //如果发现当前线程ID<=maxTaxId,说明当数据已经在处理,没必要继续操作,所以return返回
                if (localTaxId <= maxTaxId) {
                    return;
                }
                //已经有线程在等待写数据,return返回
                if (isWait) {
                    return;
                }
                //因为isSyncRunning=true,说明有线程在写数据,当前线程
                //的事务ID>maxTaxId,可以写数据,但是必须等待之前的线程处理完
                //设置标志位isWait = true,告诉其他线程,我已经在等待写数据了
                //你们不用处理了
                isWait = true;
                while (isSyncRunning) {
                    try {
                        //一直等待
                        //wait写数据的线程释放锁
                        this.wait(1000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                //这个线程可以写数据了,设置标志位isWait=false
                //告诉下一个要写数据的线程可以等待了
                isWait = false;
            }
            //交换内存,syncBuffer里有3条数据,syncBuffer(1,2,3)
            doubleBuffer.exchange();
            //最大的事务id号 maxTaxId = 3
            if (doubleBuffer.syncBuffer.size() > 0) {
                maxTaxId = doubleBuffer.getMaxTaxId();
            }
            //内存交换后需要刷新数据,设置标志位isSyncRunning=false,告诉其他线程正在同步数据
            isSyncRunning = true;
        }
        //释放锁
        //把数据持久化到磁盘，比较耗费性能的。
        //将数据1 2 3写到磁盘
        doubleBuffer.flush();
        //分段加锁
        synchronized (this) {
            //修改标志位,告诉其他线程写数据已经完成
            isSyncRunning = false;
            //唤醒正在等待的线程
            this.notifyAll();
        }
    }
}
