package com.peng.nio;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ChannelTest {

    @Test
    public void test2() {

        try {
            long start = System.currentTimeMillis();
            //使用直接缓冲区完成文件的复制(基内存映射文件)
            FileChannel inChannel = FileChannel.open(Paths.get("/Users/xingyeah/IdeaProjects/comkaikebanio/src/test/java/xx.mp4"), StandardOpenOption.READ);
            FileChannel outChannel = FileChannel.open(Paths.get("/Users/xingyeah/IdeaProjects/comkaikebanio/src/test/java/yy.mp4"),StandardOpenOption.WRITE,StandardOpenOption.READ,StandardOpenOption.CREATE);

            //进行内存映射文件
            MappedByteBuffer inMappedBuf = inChannel.map(FileChannel.MapMode.READ_ONLY,0,inChannel.size());
            MappedByteBuffer outMappedBuf = outChannel.map(FileChannel.MapMode.READ_WRITE,0,inChannel.size());

            //对缓冲区进行数据的读写操作
            byte[] dst = new byte[inMappedBuf.limit()];
            inMappedBuf.get(dst);
            outMappedBuf.put(dst);

            //回收资源
            inChannel.close();
            outChannel.close();

            long end = System.currentTimeMillis();
            System.out.println("Map最终耗时为:" + (end - start));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1(){
        //1. 创建输入输出流对象
        long start = System.currentTimeMillis();
        FileInputStream fis = null;
        FileOutputStream fos = null;
        FileChannel inChannel = null;
        FileChannel outChannel = null;

        try {
            fis = new FileInputStream("/Users/xingyeah/IdeaProjects/comkaikebanio/src/test/java/xx.mp4");
            fos = new FileOutputStream("/Users/xingyeah/IdeaProjects/comkaikebanio/src/test/java/yy.mp4");

            //2. 通过流对象获取通道对象channel
            inChannel = fis.getChannel();
            outChannel = fos.getChannel();

            //3. 创建指定大小的缓冲区对相同
            ByteBuffer buf = ByteBuffer.allocate(1024);
            //4. 将通道中的数据存入到缓冲区中
            while (inChannel.read(buf) != -1){
                buf.flip(); // 切换到读取数据的模式
                //5. 将缓冲区中的数据写入输出通道
                outChannel.write(buf);
                buf.clear(); // 清空缓冲区
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //6. 回收资源
        if(outChannel != null){
            try {
                outChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if(inChannel != null){
            try {
                inChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if(fos != null){
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(fis != null){
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        long end = System.currentTimeMillis();
        System.out.println("xx最终耗时为:" + (end - start));
    }

}
