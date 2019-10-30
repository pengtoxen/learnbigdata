package com.peng.nio;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;

public class PipeTest {

    @Test
    public void test1(){
        try {
            //1. 获取管道
            Pipe pipe = Pipe.open();

            //2. 创建缓冲区对象
            ByteBuffer buf = ByteBuffer.allocate(1024);
            //获取sink通道
            Pipe.SinkChannel sinkChannel = pipe.sink();
            buf.put("通过单向管道发送数据".getBytes());
            buf.flip();//变为读取模式
            //将buf中的数据写入到sinkChannel
            sinkChannel.write(buf);

            //3. 读取缓冲区中的数据
            Pipe.SourceChannel sourceChannel = pipe.source();
            //4. 读取sourceChannle中的数据放入到缓冲区中
            buf.flip();
            int len = sourceChannel.read(buf);
            System.out.println(new String(buf.array(),0,len));

            sourceChannel.close();
            sinkChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
