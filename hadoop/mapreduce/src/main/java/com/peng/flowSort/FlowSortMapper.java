package com.peng.flowSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, Text> {
    Text v = new Text();
    FlowSortBean flowSortBean = new FlowSortBean();
    //13480253104	180	180	360
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] fields = line.split("\t");
        String phone = fields[0];
        long upflow = Long.parseLong(fields[1]);
        long downflow = Long.parseLong(fields[2]);
        //封装对象
        flowSortBean.set(upflow, downflow);
        v.set(phone);
        context.write(flowSortBean, v);
    }
}
