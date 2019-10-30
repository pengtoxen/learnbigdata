package com.peng.reverseIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReverseIndexTwoReducer extends Reducer<Text, Text, Text, Text>{
//    atguigu	c.txt-->2
//    atguigu	b.txt-->2
//    atguigu	a.txt-->3
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text value : values){
            sb.append(value.toString() + "\t");
        }
        context.write(key, new Text(sb.toString()));
    }
}
