package com.peng.groupingComparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;
    private double price;

    public OrderBean() {
    }


    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId +  "\t" + price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * 二次排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        int result;
        if (this.orderId > o.orderId){
            result = 1;
        }else if (this.orderId < o.orderId){
            result = -1;
        }else {
            result = this.price > o.price ? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        price = in.readDouble();
    }
}
