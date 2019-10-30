package com.peng.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包的路径
        job.setJarByClass(FlowDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.关联mapper的输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.关联最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入数据的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7.设置输出数据的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置分区类
        job.setPartitionerClass(FlowPartitioner.class);
        //设置reduceTask的个数
        job.setNumReduceTasks(6);
        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
