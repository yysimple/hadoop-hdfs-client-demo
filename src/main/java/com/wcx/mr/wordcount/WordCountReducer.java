package com.wcx.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述: reduce阶段
 *
 * 前面两个参数是 map阶段输出的 key == value 类型
 * 后面两个参数是 reduce阶段自己输出的 kv类型
 *
 * @author: WuChengXing
 * @create: 2020-11-14 10:34
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 初始化返回的值，sum == 2 <wcx, 2>
     */
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 累加求和
        int sum = 0;
        // 遍历值，进行求和
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
        }
        // 设置值
        v.set(sum);
        // 写出
        context.write(key, v);
    }
}
