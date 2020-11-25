package com.wcx.mr.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-25 23:16
 **/
public class OneFriendReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        // values == A,B,C....
        //1 拼接
        for(Text person: values){
            System.out.println("key: ==> " + key.toString());
            System.out.println("values: ==>" + person.toString());
            System.out.println("---------------------------------------");
            sb.append(person).append(",");
        }

        //2 写出
        context.write(key, new Text(sb.toString()));
    }

}
