package com.atguigu;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 生产数据
 * Created by MiYang on 2018/6/1.
 */
public class Producer {
    private String start = "2019-01-01";
    private String end = "2020-01-01";
    ArrayList<String> phoneNum = new ArrayList<String>();
    Map<String, String> phoneName = new HashMap<String, String>();

    public void init() {
        phoneNum.add("18288760031");
        phoneNum.add("16818199998");
        phoneNum.add("15588997775");
        phoneNum.add("19813075419");
        phoneNum.add("15901389992");
        phoneNum.add("16203223352");
        phoneNum.add("17793507294");
        phoneNum.add("17725261264");
        phoneNum.add("14578574884");
        phoneNum.add("13775740482");
        phoneNum.add("18536773179");
        phoneNum.add("17515331243");
        phoneNum.add("13263815056");
        phoneNum.add("18313304642");
        phoneNum.add("16684904287");
        phoneNum.add("14950088430");
        phoneNum.add("18818422110");
        phoneNum.add("14278130453");
        phoneNum.add("14668237762");
        phoneNum.add("17221586046");

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

    //caller,callername,callee,calleename,build_time,build_time_ts,duration
    public String productLog() throws Exception {
        String caller;
        String callee;
        String buildTime;
        int dura;

        //1.随机生成两个不同的电话号
        int callerIndex = (int) (Math.random() * phoneNum.size());
        caller = phoneNum.get(callerIndex);

        while (true) {
            int calleeIndex = (int) (Math.random() * phoneNum.size());
            callee = phoneNum.get(calleeIndex);
            if (callerIndex != calleeIndex) break;
        }

        //2.随机生成通话建立时间（start,end）
        buildTime = randomBuildTime(start, end);

        //3.随机生成通话时长(s)
        dura = (int) (Math.random() * 30 * 60) + 1;
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format(dura);

        return caller + "," + callee + "," + buildTime + "," + duration + "\n";
    }

    //随机生成通话建立时间
    private String randomBuildTime(String start, String end) throws Exception {

        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startPoint = sdf1.parse(start).getTime();
//        System.out.println("startPoint:"+startPoint);
        long endPoint = sdf1.parse(end).getTime();

        long resultTS = startPoint + (long) (Math.random() * (endPoint - startPoint));
//        System.out.println("resultTS:"+resultTS);
        return sdf2.format(new Date(resultTS));
    }

    public void writeLog(String path) throws Exception {
        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter osw = new OutputStreamWriter(fos);

        while (true) {
            String log = productLog();
            System.out.println(log);
            osw.write(log);
            osw.flush();
            Thread.sleep(300);
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            System.out.println("没有传进路径参数");
            System.exit(0);
        }
        Producer producer = new Producer();
        producer.init();
        producer.writeLog(args[0]);//最终文件的输出路径
    }
}