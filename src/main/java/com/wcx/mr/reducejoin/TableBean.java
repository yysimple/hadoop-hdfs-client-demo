package com.wcx.mr.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-22 19:02
 **/
public class TableBean implements Writable {
    /**
     * 订单id
     */
    private String orderId;

    /**
     * 产品id
     */
    private String pId;

    /**
     * 产品数量
     */
    private int amount;

    /**
     * 产品名称
     */
    private String pName;

    /**
     * 表的标记
     */
    private String flag;

    public TableBean() {
        super();
    }

    public TableBean(String orderId, String pId, int amount, String pName, String flag) {

        super();

        this.orderId = orderId;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.pId = in.readUTF();
        this.amount = in.readInt();
        this.pName = in.readUTF();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "\t" + pName + "\t" + amount + "\t" ;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
