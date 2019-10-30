package com.peng.nio;

import org.junit.Test;

import java.nio.ByteBuffer;

public class BufferTest {
    /*
    * 管理方式几乎一致, 可以通过allocate() 获取缓冲区
    *
    * 缓冲区提供了两个核心方法: put()存入数据到缓冲区, get()获取缓冲区的数据
    *
    *2. 直接缓冲区和非直接缓冲区
    * 非直接缓冲区: 通过allocate()方法分配缓冲区, 将缓冲区建立在JVM的内存中
    * 直接缓冲区: 通过allocateDircet方法分配缓冲区, 将缓冲区建立在物理内存中, 效率更高
    * */

    @Test
    public void test3(){
        // 分配直接缓冲区
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        System.out.println(buf.isDirect());
    }


    @Test
    public void  test2(){
        String str = "abcde";
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.put(str.getBytes());
        // 切换到读取数据模式
        buf.flip();
        byte[] dst = new byte[buf.limit()];
        buf.get(dst,0,2);
        System.out.println(new String(dst,0,2));
        System.out.println(buf.position());
        // mark() 标记
        buf.mark();

        //继续读取数据
        buf.get(dst,2,2);
        System.out.println(new String(dst,2,2));
        System.out.println(buf.position());

        // reset() 恢复到mark位置
        buf.reset();
        System.out.println(buf.position());

        // 判断缓冲区当中是否还有剩余的数据
        if (buf.hasRemaining()){
            //remaining 还可以操作的数据的数量
            System.out.println(buf.remaining());
        }


    }

//    @Test
    public void test1(){
        //1. 创建缓冲区 allocate
        ByteBuffer buf = ByteBuffer.allocate(1024);
        System.out.println("-------------allocate------------");
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.capacity());

        //2. 通过put方法 想缓冲区中存入数据
        String str = "abcde";
        buf.put(str.getBytes());
        System.out.println("-------------put------------");
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.capacity());

        //3. 通过get方法, 获取缓冲区的数据, 前提是需要切换缓冲区 的模式
        buf.flip();
        System.out.println("-------------flip------------");
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.capacity());

        // 4. 获取数据
        // 创建字节数组
        byte[] dst = new byte[buf.limit()];
        buf.get(dst);
        System.out.println(new String(dst,0,dst.length));
        System.out.println("-------------get------------");
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.capacity());

        // rewind(), 可重复读
        buf.rewind();
        System.out.println("-------------rewind------------");
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.capacity());

        //clear() 清空缓冲区, 但是缓冲区的数据依然存在, 但是出于被遗忘状态
        buf.clear();
        System.out.println("-------------clear------------");
        System.out.println(buf.position());
        System.out.println(buf.limit());
        System.out.println(buf.capacity());

        System.out.println((char)buf.get());

    }
}
