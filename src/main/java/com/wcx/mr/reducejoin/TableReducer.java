package com.wcx.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 项目: hadoop-hdfs-client-demo
 * <p>
 * 功能描述:
 *
 * @author: WuChengXing
 * @create: 2020-11-22 19:08
 **/
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context)	throws IOException, InterruptedException {

        // 1准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();

        // 2 准备bean对象
        TableBean pdBean = new TableBean();

        for (TableBean bean : values) {

            /**
             * 订单表
             */
            if ("order".equals(bean.getFlag())) {

                // 拷贝传递过来的每条订单数据到集合中
                TableBean orderBean = new TableBean();

                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                orderBeans.add(orderBean);
                // 产品表
            } else {

                try {
                    // 拷贝传递过来的产品表到内存中
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 3 表的拼接
        for(TableBean bean:orderBeans){

            bean.setpName (pdBean.getpName());

            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }
    }

}
