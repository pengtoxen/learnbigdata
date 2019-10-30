package com.peng.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;
    FileSystem fileSystem = null;
    public LogRecordWriter(TaskAttemptContext job) {
        Configuration configuration = job.getConfiguration();
        try {
            //获取客户端
            fileSystem = FileSystem.get(configuration);
            Path atguiguPath = new Path("f:/atguigu.txt");
            Path otherPath = new Path("f:/other.txt");
            //  输出流
            atguiguOut = fileSystem.create(atguiguPath);
            otherOut = fileSystem.create(otherPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //获取数据
        String line = key.toString();
        //判断是否包含atguigu
        if(line.contains("atguigu")){
            atguiguOut.write(line.getBytes());
        }else {
            otherOut.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
        fileSystem.close();
    }
}
