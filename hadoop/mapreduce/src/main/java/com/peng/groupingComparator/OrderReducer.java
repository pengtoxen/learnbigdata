package com.peng.groupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean,NullWritable> {
    //(00001 123, null)(00001 34, null)(00001 12, null)
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
