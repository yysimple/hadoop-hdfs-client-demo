package com.wcx.mr.oneindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-25 21:37
 **/
public class OneIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "E:\\test\\hadoop\\oneindex\\input", "E:\\test\\hadoop\\oneindex\\output" };

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(OneIndexDriver.class);

        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
