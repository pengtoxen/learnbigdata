package com.peng.nio;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

public class NIOTest {

    @Test
    public void client() {
        try {
            //1 获取通道,默认是阻塞
            SocketChannel sChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 9898));
            //1.2 将阻塞的套接字变为非阻塞
            sChannel.configureBlocking(false);
            //2 创建指定大小的缓冲区
            ByteBuffer buf = ByteBuffer.allocate(1024);
            //3 发送数据给服务端,直接将数据存储到buf
            Scanner scan = new Scanner(System.in);
            while (scan.hasNext()) {
                String str = scan.next();
                if (str == null) {
                    continue;
                }
                buf.put((new Date().toString() + ":   " + str).getBytes());
                //将缓冲区变为读模式
                buf.flip();
                //4 将缓冲区中的数据写入到sChannel当中
                sChannel.write(buf);
                buf.clear();
            }
            //关闭通道
            sChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void server() {

        try {
            //1 获取通道
            ServerSocketChannel ssChannel = ServerSocketChannel.open();
            //2 将阻塞的套接字设置为非阻塞
            ssChannel.configureBlocking(false);
            //3 绑定端口号
            ssChannel.bind(new InetSocketAddress(9898));
            //4 创建选择器对象
            Selector selector = Selector.open();
            //5 将通道注册到选择器上,此时选择器就是开始监听这个通道的接收事件,如果有接收,接收准备就绪,才开始下一步操作
            //向选择器注册特定的事件类型,就可以在后续流程中轮询判断处理
            ssChannel.register(selector, SelectionKey.OP_ACCEPT);
            //6 通过轮询的方式获取选择器上准备就绪的事件
            //如果大于0,至少有一个selectionKey准备就绪
            while (true) {
                if (selector.select() == 0) {
                    continue;
                }
                //7 获取当前选择器中所注册的选择键(已经就绪的监听事件)
                //比如上面注册的accept事件
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                //迭代获取已经就绪的选择键
                while (it.hasNext()) {
                    //9 获取已经准备就绪的事件
                    SelectionKey sk = it.next();
                    //这里检查ssChannel是否就绪
                    if (sk.isAcceptable()) {
                        acceptHandler(selector,ssChannel);
                    } else if (sk.isReadable()) {
                        readHandler(sk);
                    }
                    //当selectKey使用完毕之后需要移除,否则会一直有效
                    it.remove();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * read事件处理
     * @param sk
     * @throws IOException
     */
    private void readHandler(SelectionKey sk) throws IOException {
        //12 如果读状态已经准备就绪,就开始读取数据
        //获取当前选择器上读状态准备就绪的通道
        SocketChannel sChannel = (SocketChannel) sk.channel();
        //读取客户端发送的数据,需要先创建缓冲区
        ByteBuffer buf = ByteBuffer.allocate(1024);
        //13 读取缓冲区数据
        int len = 0;
        while ((len = sChannel.read(buf)) > 0) {
            buf.flip();
            System.out.println(new String(buf.array(), 0, len));
            //清空缓冲区
            buf.clear();
        }
    }

    /**
     * accept事件处理
     * @param selector
     * @param ssChannel
     * @throws IOException
     */
    private void acceptHandler(Selector selector, ServerSocketChannel ssChannel) throws IOException {
        //调用accept()返回连接的channel
        SocketChannel sChannel = ssChannel.accept();
        //10 将sChannel设置为非阻塞
        sChannel.configureBlocking(false);
        //11 将该通道注册到选择器上,选择器能够监听这个通道,同样也需要完成注册
        sChannel.register(selector, SelectionKey.OP_READ);
    }
}