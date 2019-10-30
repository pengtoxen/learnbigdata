package com.peng.groupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGrouping extends WritableComparator {
    protected OrderGrouping() {
        super(OrderBean.class, true);
    }

    /**
     * reduce默认的分组规则是key相同才分组,那么如果key是一个bean,只有所有的成员对象都相同
     * 才算是相同的key.我们可以用GroupingComparator对分组进行设置,重写compare方法,满足
     * 例如这个例子中,只要bean对象的orderid一样,那么就判断key一样
     * 这样子就可以告诉reduce,把不同的key分在一组中
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        if (aBean.getOrderId() > bBean.getOrderId()) {
            result = 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }
}
