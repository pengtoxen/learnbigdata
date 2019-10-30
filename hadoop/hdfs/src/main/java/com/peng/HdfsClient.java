package com.peng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    /**
     * HDFS创建文件夹
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        // 参数优先级排序:（1）客户端代码中设置的值 >（2）classpath下的用户自定义配置文件 >（3）然后是服务器的默认配置
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");
        //2.创建目录
        fs.mkdirs(new Path("/studybigdata/hadoop/hdfs"));
        //3.关闭资源
        fs.close();
    }

    /**
     * HDFS删除文件夹
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void rmdirs() throws URISyntaxException, IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");
        //2.执行删除
        fs.delete(new Path("/studybigdata/hadoop/hdfs"), true);
        //3.关闭资源
        fs.close();
    }

    /**
     * HDFS文件上传
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void copyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("e:/hello.txt"), new Path("/hello.txt"));
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }

    /**
     * HDFS文件上传
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void copyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/hello1.txt"), new Path("e:/hello1.txt"), true);

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS文件重命名
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void rename() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 修改文件名称
        fs.rename(new Path("/hello.txt"), new Path("/hello6.txt"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS查看文件
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void listFiles() throws IOException, InterruptedException, URISyntaxException {
        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("----------------分割线-----------");
        }
    }

    /**
     * HDFS查看文件状态
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void listStatus() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }

    /**
     * HDFS IO流操作 文件上传
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hello4.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * HDFS IO流操作 文件下载
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hello1.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hello1.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 定位下载HDFS文件的第1个block块
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 定位下载HDFS文件的第2个block块
     * 文件合并
     * 在window命令窗口中执行
     * type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), configuration, "hadoop");

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 定位输入数据位置
        fis.seek(1024 * 1024 * 128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
