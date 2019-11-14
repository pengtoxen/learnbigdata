package com.peng.hadoop_rpc_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * RPC客户端
 * 调用服务端的代码
 *
 * @author Administrator
 */
public class DFSClient {
    public static void main(String[] args) throws IOException {
        //获取服务端的代理
        ClientProtocol namenode = RPC.getProxy(
                //客户端协议
                ClientProtocol.class,
                1235L,
                //服务端的主机名和端口号
                new InetSocketAddress("localhost", 9999),
                new Configuration());

        //调用服务端的makeDir方法
        //服务端执行了下面的命令
        //hadoop fs -mkdir tunning/src/testdata/output/dir
        namenode.makeDir("tunning/src/testdata/output/dir");
        //客户端运行完,进程就消失了
    }
}
