package com.wcx.mr.oneindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-25 21:32
 **/
public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum  = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        IntWritable intWritable = new IntWritable();
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}

