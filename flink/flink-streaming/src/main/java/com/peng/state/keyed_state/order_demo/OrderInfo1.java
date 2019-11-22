package com.peng.state.keyed_state.order_demo;

/**
 * 订单1中的数据格式
 * 订单号,购买的商品,商品的价格
 * 可以认为数据在kafka里的一个topic里
 * 123,拖把,30.0
 * 234,牙膏,20.0
 * 345,被子,114.4
 * 333,杯子,112.2
 * 444,Mac电脑,30000.0
 *
 * @author Administrator
 */
public class OrderInfo1 {

    //订单号
    private Long orderId;
    //商品
    private String productName;
    //价格
    private double price;

    /**
     * 对每行数据进行处理
     *
     * @param line
     * @return
     */
    public static OrderInfo1 line2Info1(String line) {
        OrderInfo1 orderInfo1 = new OrderInfo1();
        if (line == null) {
            return orderInfo1;
        }
        String[] fields = line.split(",");
        orderInfo1.setOrderId(Long.parseLong(fields[0]));
        orderInfo1.setProductName(fields[1]);
        orderInfo1.setPrice(Double.parseDouble(fields[2]));
        return orderInfo1;
    }


    public OrderInfo1() {

    }

    @Override
    public String toString() {
        return "OrderInfo1{" +
                "orderId=" + orderId +
                ", productName='" + productName + '\'' +
                ", price=" + price +
                '}';
    }

    public OrderInfo1(Long orderId, String productName, double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
