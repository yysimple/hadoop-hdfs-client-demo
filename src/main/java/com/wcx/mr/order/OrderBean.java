package com.wcx.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-22 16:32
 **/
public class OrderBean implements WritableComparable<OrderBean> {

    /**
     * 订单id号
     */
    private int orderId;

    /**
     * 价格
     */
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int orderId, double price) {
        super();
        this.orderId = orderId;
        this.price = price;
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

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public int getOrder_id() {
        return orderId;
    }

    public void setOrder_id(int order_id) {
        this.orderId = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * 自定义排序规则
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {

        int result;

        if (orderId > o.getOrder_id()) {
            result = 1;
        } else if (orderId < o.getOrder_id()) {
            result = -1;
        } else {
            // 价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

}
