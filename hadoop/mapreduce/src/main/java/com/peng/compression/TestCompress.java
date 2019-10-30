package com.peng.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestCompress {
    public static void main(String[] args) throws Exception {
    }

    /**
     * 解压
     *
     * @throws IOException
     */
    @Test
    public void deCompress() throws IOException {
        String name = "F:\\learnbigdata\\hadoop\\rawdata\\Compression\\uncompression.avi.gz";
        //1.获取输入流
        FileInputStream fis = new FileInputStream(name);
        //2.获取编码器
        Configuration configuration = new Configuration();
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = codecFactory.getCodec(new Path(name));
        if (codec == null) {
            System.out.println("can not find codec for this file" + name);
            return;
        }
        //3.获取压缩流
        CompressionInputStream cis = codec.createInputStream(fis);
        //4.获取输出流
        FileOutputStream fos = new FileOutputStream(name + ".decode");
        //5.流的拷贝
        IOUtils.copyBytes(cis, fos, configuration);
        //6.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cis);
    }

    /**
     * 压缩
     *
     * @throws Exception
     */
    @Test
    public void compress() throws Exception {
        String name = "F:\\learnbigdata\\hadoop\\rawdata\\Compression\\uncompression.avi";
        String method = "org.apache.hadoop.io.compress.GzipCodec";
        //1.获取输入流
        FileInputStream fis = new FileInputStream(name);
        //2.获取编解码器
        Class clazz = Class.forName(method);
        Configuration configuration = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, configuration);
        //3.获取文件输出流
        FileOutputStream fos = new FileOutputStream(name + codec.getDefaultExtension());
        //4.获取压缩流
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //5.流的拷贝
        IOUtils.copyBytes(fis, cos, configuration);
        //6.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fis);
    }

}
