package com.peng.namenode_demo;

import java.util.LinkedList;

/**
 * 双缓存区
 * 当currentBuffer里面有数据,需要刷写磁盘的时候
 * 就和syncBuffer交换内存地址
 * currentBuffer里的数据由一个线程写磁盘
 * syncBuffer接收其他线程往里塞数据
 */
class DoubleBuffer {

    //写数据,有序队列
    LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();
    //用来把数据持久化到磁盘上面的内存
    LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

    /**
     * 写元数据信息
     *
     * @param editLog
     */
    void write(EditLog editLog) {
        currentBuffer.add(editLog);
    }

    /**
     * 把数据写到磁盘
     */
    void flush() {
        for (EditLog editLog : syncBuffer) {
            //把打印出来，我们就认为这就是写到磁盘了
            System.out.println(editLog);
        }
        syncBuffer.clear();
    }

    /**
     * 交换一下内存
     */
    void exchange() {
        LinkedList<EditLog> tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    /**
     * 获取到正在同步数据的内存里面事务ID最大的ID
     *
     * @return
     */
    long getMaxTaxId() {
        return syncBuffer.getLast().taxId;
    }
}
