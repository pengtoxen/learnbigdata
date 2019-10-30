package com.peng.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * 相当于是yarn客户端，负责提交mapreduce程序
 */
public class WordcountCompressionDriver {
    @Test
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job对象
        Configuration configuration = new Configuration();

        // 开启map端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);
        //2.指定jar包路径
        job.setJarByClass(WordcountCompressionDriver.class);
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

        //设置combiner类, 如果合并逻辑和reducer一样,可以直接用reducer
        //一般用于求和操作
        job.setCombinerClass(WordcountCombiner.class);


        // 设置reduce输出压缩格式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
	    //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
