package com.peng.grouping_comparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        Integer orderId = orderBean.getOrderId();
        return (orderId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
