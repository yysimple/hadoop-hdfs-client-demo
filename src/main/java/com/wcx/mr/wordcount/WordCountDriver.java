package com.wcx.mr.wordcount;

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
 * 功能描述: 将map和reduce结合起来
 *
 * @author: WuChengXing
 * @create: 2020-11-14 10:44
 **/
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1. 获取job对象
        Job job = Job.getInstance(conf);

        // 2. 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        // 3. 关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 设置Map阶段输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 输出路径不能存在 FileAlreadyExistsException: Output directory file:/E:/test/hadoop/wordcount/output already exists
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
