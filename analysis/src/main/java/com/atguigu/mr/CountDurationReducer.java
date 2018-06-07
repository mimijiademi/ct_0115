package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by MiYang on 2018/6/4.
 */
public class CountDurationReducer extends Reducer<CommDimension, Text, CommDimension, CountDurationValue> {

    private CountDurationValue v = new CountDurationValue();

    @Override
    protected void reduce(CommDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //通话总次数
        int countSum = 0;
        //通话总时长
        int durationSum = 0;
        //循环累加
        for (Text value : values) {
            //每过来一个通话时长代表有过一次通话，累加
            countSum++;
            //累加通话时长
            durationSum += Integer.valueOf(value.toString());
        }
        //设置value的值，int类型转化成String
        v.setCountSum(countSum + "");
        v.setDurationSum(durationSum + "");
        //写出去
        context.write(key, v);
    }
}
