package com.peng.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, NullWritable, TableBean> {
    //1001  01  1   -> 订单表数据
    //01    小米  -> 商品表数据
    //1001  小米  1   -> join后的数据
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //用来接收order表的数据
        List<TableBean> orderBeans = new ArrayList<>();
        //用来接受pd表的数据
        TableBean pBean = new TableBean();
        for (TableBean value : values){
            //order表
            if (value.getFlag().equals("0")){
                TableBean oBean = new TableBean();
                try {
                    //拷贝属性,不能直接用value,因为value是个指针
                    BeanUtils.copyProperties(oBean, value);
                    orderBeans.add(oBean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                //pd.txt
                try {
                    BeanUtils.copyProperties(pBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean orderbean : orderBeans){
            orderbean.setPname(pBean.getPname());
            context.write(NullWritable.get(),orderbean);
        }
    }
}
