package com.wcx.mr.oneindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-25 21:50
 **/
public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        /**
         * atguigu
         */
        for (Text value : values) {
            stringBuilder.append(value.toString().replace("\t", "-->") + "\t");
        }
        k.set(key);
        v.set(stringBuilder.toString());
        context.write(k, v);

    }
}
