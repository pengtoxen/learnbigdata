package com.peng.sharedFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SharedFriendsTwoReducer extends Reducer<Text, Text, Text, Text> {
    //A-B C A-B D A-B E
    //A-B	E C
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        for (Text friend : values){
            buffer.append(friend + "\t");
        }
        context.write(key, new Text(buffer.toString()));
    }
}
