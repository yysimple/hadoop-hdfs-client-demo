package com.wcx.mr.oneindex;

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
 * @create: 2020-11-25 21:47
 **/
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text v = new Text();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("--");
        k.set(split[0]);
        v.set(split[1]);
        context.write(k, v);
    }
}
