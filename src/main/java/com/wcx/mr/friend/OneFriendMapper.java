package com.wcx.mr.friend;

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
 * @create: 2020-11-25 23:15
 **/
public class OneFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        // 1 获取一行 A:B,C,D,F,E,O
        String line = value.toString();

        // 2 切割
        String[] fields = line.split(":");

        // 3 获取person和好友
        String person = fields[0];
        String[] friends = fields[1].split(",");

        // 4写出去
        for(String friend: friends){
            // B  <==> A
            // C  <==> A
            // 输出 <好友，人>
            System.out.println("key: ==> " + friend);
            System.out.println("values: ==>" + person);
            System.out.println("---------------------------------------");
            context.write(new Text(friend), new Text(person));
        }
    }

}
