package com.wcx.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述: mapper类 == map阶段
 *
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT

 * KEYIN: 输入数据的key的类型
 * VALUEIN：输入数据值的类型
 * KEYOUT：输出数据的key的类型
 * VALUEOUT：输出数据的值的类型
 *
 *
 * @author: WuChengXing
 * @create: 2020-11-14 10:17
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 初始化返回的key == wcx   <wcx, 1>
      */
    Text k = new Text();

    /**
     * 初始化返回的值，默认是1  <wcx, 1>
     */
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取文本中的第一行，map默认是以行为单位进行读取数据 如 // wcx wcx
        String line = value.toString();
        // 切割，以空格切割字符串
        String[] s = line.split(" ");
        for (String word : s) {
            // 设置key
            k.set(word);
            // 将分割好的结果写出
            context.write(k, v);
        }
    }
}
