package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by MiYang on 2018/6/5 15:42.
 */
public class CountDurationDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置&job对象
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);

        //设置jar
        job.setJarByClass(CountDurationMapper.class);
        Scan scan = new Scan();
        //设置mapper
        TableMapReduceUtil.initTableMapperJob("ct:calllog", scan, CountDurationMapper.class, CommDimension.class, Text.class, job);

        //设置reducer
        job.setReducerClass(CountDurationReducer.class);

        //设置输出类型&自定义的OutPutFormat
        job.setOutputFormatClass(MySQLOutPutFormat.class);

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
