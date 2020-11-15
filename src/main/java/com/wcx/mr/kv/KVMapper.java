package com.wcx.mr.kv;

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
 * @create: 2020-11-15 09:49
 **/
public class KVMapper extends Mapper<Text, Text, Text, LongWritable> {
    /**
     *  1 设置value
     */
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        // 2 写出
        context.write(key, v);
    }

}
