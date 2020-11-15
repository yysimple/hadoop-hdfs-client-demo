package com.wcx.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-14 22:00
 **/
public class FlowBeanMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    /**
     * key 为手机号
     */
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每行数据
        String line = value.toString();
        // 分割line
        String[] split = line.split("\t");
        // 封装成一个对象
        k.set(split[1]);
        FlowBean flowBean = new FlowBean();
        // 设置上行流量
        flowBean.setUpFlow(Long.parseLong(split[split.length - 3]));
        // 设置下行流量
        flowBean.setDownFlow(Long.parseLong(split[split.length - 2]));
        context.write(k, flowBean);
    }
}
