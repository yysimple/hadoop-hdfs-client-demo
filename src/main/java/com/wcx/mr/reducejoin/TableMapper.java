package com.wcx.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-22 19:06
 **/
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    TableBean bean = new TableBean();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 1 获取输入文件切片
        FileSplit split = (FileSplit) context.getInputSplit();

        // 2 获取输入文件名称
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取输入数据
        String line = value.toString();

        // 2 不同文件分别处理
        if (name.startsWith("order")) {// 订单表处理

            // 2.1 切割
            String[] fields = line.split("\t");

            // 2.2 封装bean对象
            bean.setOrderId(fields[0]);
            bean.setpId(fields[1]);
            bean.setAmount(Integer.parseInt(fields[2]));
            bean.setpName("");
            bean.setFlag("order");

            k.set(fields[1]);
        }else {// 产品表处理

            // 2.3 切割
            String[] fields = line.split("\t");

            // 2.4 封装bean对象
            bean.setpId(fields[0]);
            bean.setpName(fields[1]);
            bean.setFlag("pd");
            bean.setAmount(0);
            bean.setOrderId("");

            k.set(fields[0]);
        }

        // 3 写出
        context.write(k, bean);
    }

}
