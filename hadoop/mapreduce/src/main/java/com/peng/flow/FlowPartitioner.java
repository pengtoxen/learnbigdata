package com.peng.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class FlowPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //获取手机号前三位
        String num = text.toString().substring(0, 3);
        //判断
        int parition = 4;
        if("136".equals(num)){
            parition = 0;
        }else if ("137".equals(num)){
            parition = 1;
        }else if ("138".equals(num)){
            parition = 2;
        }else if ("139".equals(num)){
            parition = 3;
        }
        return parition;
    }
}
