package com.wcx.mr.oneindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-25 21:28
 **/
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String name;
    IntWritable v = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每一行
        String line = value.toString();
        // 切割
        String[] s = line.split(" ");
        Text k = new Text();
        for (String word : s) {
            k.set(word + "--" + name);
            v.set(1);
            context.write(k, v);
        }

    }
}
