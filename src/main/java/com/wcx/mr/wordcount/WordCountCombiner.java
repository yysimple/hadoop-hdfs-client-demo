package com.wcx.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述: map与reducer中间操作
 *
 * @author: WuChengXing
 * @create: 2020-11-16 23:59
 **/
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 1 汇总
        int sum = 0;

        for(IntWritable value :values){
            sum += value.get();
        }
        v.set(sum);

        // 2 写出
        context.write(key, v);
    }


}
