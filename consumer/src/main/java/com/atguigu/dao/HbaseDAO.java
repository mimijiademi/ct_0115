package com.atguigu.dao;

import com.atguigu.utils.ConnectInstance;
import com.atguigu.utils.HbaseUtil;
import com.atguigu.utils.PropertyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 创建namespace，创建表，put数据
 * Created by MiYang on 2018/6/2.
 */
public class HbaseDAO {

    private String namespace;//HBase命名空间
    private String tableName;//HBase表
    private int regions;//HBase分区数
    private String cf;//HBase列族,第一个列族
//    private String cfTwo;//HBase列族,第二个列族
    private SimpleDateFormat sdf = null;
    private HTable table;//操作HBase表对象
    private String flag;//主被叫数据标志位

    //缓存put对象的集合
    private List<Put> listPut;

    //在创建HbaseDAO时初始化属性及创建NS和table
    public HbaseDAO() throws Exception {
        //初始化相关属性（数据来源于配置文件kafka_hbase.properties）
        namespace = PropertyUtil.getProperty("hbase.namespace");
        tableName = PropertyUtil.getProperty("hbase.table.name");
        regions = Integer.valueOf(PropertyUtil.getProperty("hbase.regions"));
        cf = PropertyUtil.getProperty("hbase.table.cf");
//        cfTwo = PropertyUtil.getProperty("hbase.table.cfTwo");
//        flag = PropertyUtil.getProperty("hbase.table.flag");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        listPut = new ArrayList<Put>();
        flag = "1";  //可以放在配置文件kafka_hbase.properties中
        //初始化命名空间及表的创建
        if (!HbaseUtil.isTableExist(tableName)) {
//            HbaseUtil.initNamespace(namespace);
            HbaseUtil.createTable(tableName, regions, cf, "f2");
        }
    }

    /**
     * @param ori 18232595809,18842305487,2019-02-21 13:13:13,0180
     *            rowkey   xxx13651234567_2019-02-21 13:13:13_13891234567_0180
     *            regionHash_caller_buildTime_callee_duration
     *            call1,buildtime,buildtime_ts,call2,duration
     */
    public void put(String ori) throws IOException, ParseException {
        //创建连接及获取表
        if (listPut.size() == 0) {
            //获取连接（单例对象）
            Connection connection = ConnectInstance.getInstance();
            //获取表对象
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
            //设置不自动提交
            table.setAutoFlushTo(false);
            //设置客户端缓存大小，1024*1024之后提交一次
            table.setWriteBufferSize(1024 * 1024);
        }
        //如果传输的数据为空，直接返回
        if (ori == null) return;
        //ori:14314302040,19460860743,2019-05-08 23:41:05,0439
        String[] split = ori.split(",");//切分原始数据

        //截取字段封装相关参数
        String caller = split[0];//主叫
        String callee = split[1];//被叫
        String buildTime = split[2];//通话建立时间
        long time = sdf.parse(buildTime).getTime();//通话建立时间戳
        String buildtime_ts = time + "";//时间戳转换为string类型
        String duration = split[3];//通话时长

        //获取分区号
        String regionHash = HbaseUtil.getRegionHash(caller, buildTime, regions);
        String rowKey = HbaseUtil.getRowKey(regionHash, caller, buildTime, callee, flag, duration);


//        Configuration conf = HBaseConfiguration.create();
//        Connection connection = ConnectionFactory.createConnection(conf);//getInstance()
////        ConnectionFactory.createConnection(Constant.CONF);
//        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));

        //为每一条数据创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //向put中添加数据（列族：列）（值）
        // call1,buildtime,buildtime_ts,call2,duration
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildtime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildtime_ts"), Bytes.toBytes(buildtime_ts));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        //向put缓存中添加对象
        listPut.add(put);
        if (listPut.size() > 20) {
            table.put(listPut);
            //手动提交
            table.flushCommits();
            //清空list集合
            listPut.clear();
            //关闭表连接（如果业务单一，可以初始化时创建表连接，此处就不需要关闭表连接）
            table.close();
        }
    }
}
