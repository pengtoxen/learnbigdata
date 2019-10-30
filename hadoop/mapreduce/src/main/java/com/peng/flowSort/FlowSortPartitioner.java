package com.peng.flowSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class FlowSortPartitioner extends Partitioner<FlowSortBean, Text> {
    @Override
    public int getPartition(FlowSortBean flowSortBean, Text text, int numPartitions) {
        String phone = text.toString().substring(0, 3);
        int partition = 4;
        if("136".equals(phone)){
            partition = 0;
        }else if ("137".equals(phone)){
            partition = 1;
        }else if ("138".equals(phone)){
            partition = 2;
        }else if ("139".equals(phone)){
            partition = 3;
        }
        return partition;
    }
}
