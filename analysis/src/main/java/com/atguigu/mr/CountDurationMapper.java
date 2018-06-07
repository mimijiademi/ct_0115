package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.crypto.Context;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by MiYang on 2018/6/4.
 */
public class CountDurationMapper extends TableMapper<CommDimension,Text> {

    Map<String, String> phoneName = new HashMap<String, String>();
    private Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        init();
    }

    public void init(){
        phoneName.put("18288760031", "李雁");
        phoneName.put("16818199998", "卫艺");
        phoneName.put("15588997775", "仰莉");
        phoneName.put("19813075419", "陶欣悦");
        phoneName.put("15901389992", "施梅梅");
        phoneName.put("16203223352", "金虹霖");
        phoneName.put("17793507294", "魏明艳");
        phoneName.put("17725261264", "华贞");
        phoneName.put("14578574884", "华啟倩");
        phoneName.put("13775740482", "仲采绿");
        phoneName.put("18536773179", "卫丹");
        phoneName.put("17515331243", "戚丽红");
        phoneName.put("13263815056", "何翠柔");
        phoneName.put("18313304642", "钱溶艳");
        phoneName.put("16684904287", "钱琳");
        phoneName.put("14950088430", "缪静欣");
        phoneName.put("18818422110", "焦秋菊");
        phoneName.put("14278130453", "吕访琴");
        phoneName.put("14668237762", "沈丹");
        phoneName.put("17221586046", "褚美丽");
    }
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获得rowkey，型如：0x_18232595809_2019-02-21 13:13:13_18842305486_1_0180
        String rowkey = Bytes.toString(value.getRow());
        //按照"_"切割rowkey
        String[] split = rowkey.split("_");
        //rowkey已经可以得到主叫和被叫两者的数据，为了避免重复，当标志被叫在前的flag=0时，直接return
        String flag = split[4];
        if ("0".equals(flag)){
            return;
        }

        String call1 = split[1];
        String call2 = split[3];
        //buildTime的格式为2019-02-21 13:13:13
        String buildTime = split[2];
        //从buildTime中截出年份
        String year = buildTime.substring(0, 4);
        //从buildTime中截出月份
        String month = buildTime.substring(5, 7);
        //从buildTime中截出天数
        String day = buildTime.substring(8, 10);
        //截出通话时长
        String duration = split[5];
        //将通话时长封装进text类型的v，以备传出去
        v.set(duration);
        //获取Mapper的key对象
        CommDimension commDimension = new CommDimension();

        //第一个联系人【主叫】，联系人维度 封装
        ContactDimension contactDimension = new ContactDimension();
        contactDimension.setName(phoneName.get(call1));
        contactDimension.setPhoneNum(call1);
        //年维度封装
        DateDimension yearDimension = new DateDimension(year,"-1","-1");
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);
        //月维度封装
        DateDimension monthDimension = new DateDimension(year,month,"-1");
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);
        //天维度封装
        DateDimension dayDimension = new DateDimension(year,month,day);
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension,v);


        //第二个联系人【被叫】，联系人维度封装
        contactDimension.setName(phoneName.get(call2));
        contactDimension.setPhoneNum(call2);
        //年维度封装
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);
        //月维度封装
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);
        //天维度封装
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension,v);


    }
}
