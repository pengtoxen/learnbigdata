package com.peng.shared_friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SharedFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();
//    A:B,C,D,F,E,O
//    B:A,C,E,K

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        String[] split = line.split(":");
        String[] friends = split[1].split(",");
        for (String friend : friends){
            k.set(friend);
            v.set(split[0]);
            context.write(k , v);
        }
    }
}
