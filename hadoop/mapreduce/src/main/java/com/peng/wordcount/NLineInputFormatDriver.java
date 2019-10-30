package com.peng.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * 相当于是yarn客户端，负责提交mapreduce程序
 */
public class NLineInputFormatDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2.指定jar包路径
        job.setJarByClass(NLineInputFormatDriver.class);

        //3.关联mapper和reducer类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //4.指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.指定job输入的数据的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7.指定job输出的数据的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //指定InputFormat的类型,设置每个切片InputSplit中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        job.setInputFormatClass(NLineInputFormat.class);

        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
