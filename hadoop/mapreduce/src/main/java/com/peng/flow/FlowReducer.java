package com.peng.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1.累加
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean value : values){
            sumUpFlow += value.getUpflow();
            sumDownFlow += value.getDownflow();
        }
        //2.封装对象
        flowBean.set(sumUpFlow, sumDownFlow);
        //3.写出去
        context.write(key, flowBean);
    }
}
