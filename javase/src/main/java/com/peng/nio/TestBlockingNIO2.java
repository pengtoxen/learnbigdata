package com.peng.nio;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestBlockingNIO2 {

    /*
    * 使用nio完成网络通信需要三个核心对象
    *
    * 1. 通道Channel
    *  java.nio.channels.Channel接口
    *   SocketChannel
    *   ServerSocketChannel
    *   DatagramChannel
    *
    *   管道相关:
    *   Pipe.SinkChannel
    *   Pipe.SourceChannel
    *
    * 2. 缓冲buffer: 负责存储数据
    *
    * 3. Selector: 是SelectableChannel的多路复用器, 主要是用于监控SelectableChannel的IO状况
    * */

    //阻塞式的网络通信: 通过客户端socket 向服务端的socket发送图片, 服务端讲图片保存到本地
    @Test
    public void client(){
        try {
            //1. 获取通道
            SocketChannel sChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1",9898));
            // 创建文件通道
            FileChannel inChanel = FileChannel.open(Paths.get("/Users/xingyeah/IdeaProjects/comkaikebanio/src/test/java/11.jpg"), StandardOpenOption.READ);
            //2. 分配指定大小的缓冲区
            ByteBuffer buf = ByteBuffer.allocate(1024);

            //3. 发送数据, 需要读取文件: 11.jpg
            while (inChanel.read(buf) != -1){
                buf.flip(); //将buf的模式改为读模式
                sChannel.write(buf); //将buf中的数据写入通道中
                buf.clear();
            }
            sChannel.shutdownOutput(); //主动告诉服务端, 数据已经发送完毕
            int len = 0;
            while ((len = sChannel.read(buf)) != -1){
                buf.flip();
                System.out.println(new String(buf.array(), 0,len));
            }

            //4. 关闭通道
            inChanel.close();
            sChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void server(){
        //1. 获取通道
        try {
            ServerSocketChannel ssChannel = ServerSocketChannel.open();
            //1.2 创建一个输出通道, 将读取到的数据写入输出通道中,保存在22.jpg文件中
            FileChannel outChannel = FileChannel.open(Paths.get("/Users/xingyeah/IdeaProjects/comkaikebanio/src/test/java/22.jpg"),StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            //2. 绑定端口
            ssChannel.bind(new InetSocketAddress(9898));
            //3. 等待客户端连接, 当连接成功, 就会得到一个连接的通道
            SocketChannel sChannel = ssChannel.accept();
            //4. 创建缓冲区
            ByteBuffer buf = ByteBuffer.allocate(1024);

            //5. 接受客户端的数据, 并且存储到本地
            while (sChannel.read(buf) != -1){
                buf.flip();
                outChannel.write(buf);
                buf.clear();

            }

            //发送反馈给客户端
            //1. 向缓冲区中写入应答信息
            buf.put("服务端接收数据成功".getBytes());
            buf.flip();
            sChannel.write(buf);

            //关闭通道
            sChannel.close();
            outChannel.close();
            buf.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //依然是阻塞式

    }

}
