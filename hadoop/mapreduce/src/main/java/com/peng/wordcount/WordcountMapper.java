package com.peng.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * （1）用户自定义的Mapper要继承自己的父类
 * （2）Mapper的输入数据是KV对的形式（KV的类型可自定义）
 * （3）Mapper中的业务逻辑写在map()方法中
 * （4）Mapper的输出数据是KV对的形式（KV的类型可自定义）
 * （5）map()方法（maptask进程）对每一个<K,V>调用一次
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    /**
     * map()方法（maptask进程）对每一个<K,V>调用一次
     *
     * @param key:数据的offset
     * @param value：要处理的一行数据
     * @param context：上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将一行数据转化为string类型
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
