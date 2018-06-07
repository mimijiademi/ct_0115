package com.atguigu.util;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * 获得数据库链接
 * Created by MiYang on 2018/6/5 9:51.
 */
public class JDBCInstance {
    private static Connection connection=null;
    private JDBCInstance(){

    }
    public static Connection getInstance() throws SQLException {
        if (connection==null||connection.isClosed()){
            connection = (Connection) JDBCUtil.getConnection();
        }
        return connection;
    }
}
