package com.peng.hadoop_rpc_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

/**
 * 模拟RPC服务端
 *
 * @author Administrator
 */
public class NameNodeRpcServer implements ClientProtocol {

    /**
     * 创建目录
     */
    @Override
    public void makeDir(String path) {
        System.out.println("服务端创建了目录：" + path);
    }

    @Override
    public void getFile(String name) {
        System.out.println("服务端查找文件：" + name);
    }

    public static void main(String[] args) throws Exception {
        //监听ip地址和端口
        //这里是是localhost和9999
        Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(9999)
                .setProtocol(ClientProtocol.class)
                .setInstance(new NameNodeRpcServer())
                .build();

        //启动服务端,等到客户端的调用
        //jps可以看到NameNodeRpcServer进程
        server.start();
    }
}
