package com.atguigu.util;

import java.sql.*;

/**
 * JDBC编程步骤
 * Created by MiYang on 2018/6/5 9:46.
 */
public class JDBCUtil {
    private static final String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_JDBC_URL = "jdbc:mysql://hadoop101:3306/ct?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_JDBC_USERNAME = "root";
    private static final String MYSQL_JDBC_PASSWORD = "123456";

    public static Connection getConnection() {

        try {
            //（1）加载mysql驱动
            Class.forName(MYSQL_JDBC_DRIVER);
            //（2）获得数据库连接
            return DriverManager.getConnection(MYSQL_JDBC_URL, MYSQL_JDBC_USERNAME, MYSQL_JDBC_PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void close(Connection connection,Statement statement,ResultSet resultSet){
        //连接
        if (connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //编译sql
        if (statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //关闭结果集
        if (resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}
