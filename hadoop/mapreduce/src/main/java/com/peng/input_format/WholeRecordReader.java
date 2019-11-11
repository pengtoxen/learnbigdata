package com.peng.input_format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private BytesWritable value = new BytesWritable();
    private Configuration configuration ;
    private FileSplit fileSplit;
    boolean processed = false;
    /**
     * 初始化方法
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //获取配置文件
        configuration = context.getConfiguration();
        //强转类型
        fileSplit = (FileSplit) split;
    }
    /**
     * 封装key，value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[] buf = new byte[(int) fileSplit.getLength()];
            //获取fileSystem客户端
            FileSystem fileSystem = null;
            FSDataInputStream fis = null;
            try {
               fileSystem = FileSystem.get(configuration);
                //获取输入流
                Path path = fileSplit.getPath();
                fis = fileSystem.open(path);
                //读数据
                IOUtils.readFully(fis, buf, 0, buf.length);
                //将读取的内容设置到value
                value.set(buf,0, buf.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
                fileSystem.close();
            }
            //防止重复读取文件
            processed = true;
            //文件已读完，可以进入map方法
            return true;
        }
        return false;
    }

    /**
     * 获取封装好的key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取封装好的value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
