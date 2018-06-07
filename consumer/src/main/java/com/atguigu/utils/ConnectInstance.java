package com.atguigu.utils;

import com.atguigu.constants.Constant;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 *
 * 获取连接的单例
 *
 * Created by MiYang on 2018/6/2.
 */
public class ConnectInstance {
    private static Connection connection;
    //私有化构造器
    public ConnectInstance() {
    }
    public static Connection getInstance(){
        if (connection==null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(Constant.CONF);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
