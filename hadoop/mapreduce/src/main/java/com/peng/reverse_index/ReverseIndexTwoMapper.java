package com.peng.reverse_index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReverseIndexTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();
    //atguigu--c.txt 2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("--");
        String[] str = words[1].split("\t");
        k.set(words[0]);
        v.set(str[0] + "-->" + str[1]);
        context.write(k, v);
    }
}
