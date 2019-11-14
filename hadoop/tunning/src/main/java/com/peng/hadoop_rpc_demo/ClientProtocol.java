package com.peng.hadoop_rpc_demo;

/**
 * 协议其实就是接口
 * 协议里有很多抽象方法,这些方法由服务端来实现
 * 要新增功能,只要新增协议,服务端实现就可以
 *
 * @author Administrator
 */
public interface ClientProtocol {

    /**
     * 协议必须有versionID
     */
    long versionID = 1234L;

    /**
     * 创建目录
     * @param path
     */
    void makeDir(String path);

    /**
     * 查找文件
     * @param name
     */
    void getFile(String name);

}
