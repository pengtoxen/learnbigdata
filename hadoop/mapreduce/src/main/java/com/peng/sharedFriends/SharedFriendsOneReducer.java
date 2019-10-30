package com.peng.sharedFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SharedFriendsOneReducer extends Reducer<Text, Text, Text, Text> {
    //A B  A C  A D
    //A	I,K,C,B,G,F,H,O,D,
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Text person : values){
            sb.append(person + ",");
        }
        context.write(key, new Text(sb.toString()));
    }
}
