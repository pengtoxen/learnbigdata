package com.peng.shared_friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class SharedFriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text k = new Text();
        Text v = new Text();
//        A	I,K,C,B,G,F,H,O,D,
//        B	A,F,J,E,
        String line = value.toString();
        String[] friend2person = line.split("\t");
        String[] persons = friend2person[1].split(",");
        Arrays.sort(persons);
        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++){
                String person2person = persons[i] + "-" + persons[j];
                k.set(person2person);
                v.set(friend2person[0]);
                context.write(k, v);
            }
        }
    }
}
