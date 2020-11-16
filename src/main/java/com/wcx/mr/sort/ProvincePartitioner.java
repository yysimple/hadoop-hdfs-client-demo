package com.wcx.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述: 自定义分区
 *
 * @author: WuChengXing
 * @create: 2020-11-15 21:43
 **/
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    /**
     * 自定义分区规则
     *
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        // 1 获取电话号码的前三位
        String preNum = text.toString().substring(0, 3);

        int partition = 4;

        // 2 判断是哪个省
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;

    }
}
