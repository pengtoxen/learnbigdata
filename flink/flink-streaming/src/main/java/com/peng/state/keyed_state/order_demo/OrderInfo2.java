package com.peng.state.keyed_state.order_demo;

/**
 * 订单2中的数据格式
 * 订单号,下单时间,下单的地点
 * 可以认为数据在kafka里的另一个一个topic里
 * 123,2019-11-11 10:11:12,江苏
 * 234,2019-11-11 11:11:13,云南
 * 345,2019-11-11 12:11:14,安徽
 * 333,2019-11-11 13:11:15,北京
 * 444,2019-11-11 14:11:16,深圳
 *
 * @author Administrator
 */
public class OrderInfo2 {

    //订单号
    private Long orderId;
    //下单日期
    private String orderDate;
    //下单的地点
    private String address;

    /**
     * 对每行数据进行处理
     *
     * @param line
     * @return
     */
    public static OrderInfo2 line2Info2(String line) {
        OrderInfo2 orderInfo2 = new OrderInfo2();
        if (line == null) {
            return orderInfo2;
        }
        String[] fields = line.split(",");
        orderInfo2.setOrderId(Long.parseLong(fields[0]));
        orderInfo2.setOrderDate(fields[1]);
        orderInfo2.setAddress(fields[2]);
        return orderInfo2;
    }

    public OrderInfo2() {

    }

    @Override
    public String toString() {
        return "OrderInfo2{" +
                "orderId=" + orderId +
                ", orderDate='" + orderDate + '\'' +
                ", address='" + address + '\'' +
                '}';
    }

    public OrderInfo2(Long orderId, String orderDate, String address) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.address = address;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
