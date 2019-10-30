package com.peng.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * CombineTextInputFormat切片机制
 * 默认情况下TextInputformat对任务的切片机制是按文件规划切片，
 * 不管文件多小，都会是一个单独的切片，都会交给一个maptask，
 * 这样如果有大量小文件，就会产生大量的maptask，处理效率极其低下
 * <p>
 * 优化策略
 * （1）最好的办法，在数据处理系统的最前端（预处理/采集），将小文件先合并成大文件，再上传到HDFS做后续分析。
 * （2）补救措施：如果已经是大量小文件在HDFS中了，可以使用另一种InputFormat来做切片（CombineTextInputFormat），
 * 它的切片逻辑跟TextFileInputFormat不同：它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个maptask。
 */
public class CombineTextInputDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2.指定jar包路径
        job.setJarByClass(CombineTextInputDriver.class);

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

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        // 1.将小文件合并,合并后的文件<2m 算作一个切片
        // 2.将小文件合并,合并后的文件>2并且<4m 算作一个切片
        // 3.将小文件合并,合并后的文件>4m,则第一个切片=4m,剩下的部分继续判断是否合并,重复条件1-3
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m
        job.setInputFormatClass(CombineTextInputFormat.class);

        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
