package com.atguigu.convertor;

import com.atguigu.kv.base.BaseDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import com.atguigu.util.JDBCInstance;
import com.atguigu.util.JDBCUtil;
import com.atguigu.util.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * 维度对象转维度id类
 * Created by MiYang on 2018/6/5 10:29.
 */
public class DimensionConvertorImpl implements IConvertor {

    //创建数据缓存队列
    private LRUCache lruCache = new LRUCache(3000);

    /**
     * 1、判断内存缓存中是否已经有该维度的id，如果存在则直接返回该id
     * 2、如果内存缓存中没有，则查询数据库中是否有该维度id，如果有，则查询出来，返回该id，
     *    并缓存到内存中。
     * 3、如果数据库中也没有该维度id，则直接插入一条新的维度信息，成功插入后，重新查询该维度，
     *    返回该id，并缓存到内存中。
     *
     */
    public int getDimensionID(BaseDimension baseDimension) {
        //1.查询缓存是否有值
        //1)、根据传入的维度对象取得该维度对象对应的cachekey
        String cacheKey = getCacheKey(baseDimension);
        if (lruCache.containsKey(cacheKey)){
            return lruCache.get(cacheKey);//缓存中有值
        }
        //2.获取mysql中是否有值,如果没有，插入数据
        String[] sqls = getSqls(baseDimension);

        Connection connection = null;
        try {
            connection = JDBCInstance.getInstance();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //3.执行sql（查询，插入，查询）
        int id = -1;
        try {
            id=execSql(sqls,connection,baseDimension);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (id == -1) throw new RuntimeException("未匹配到相应维度!!！");

        //5.将数据放入缓存
        lruCache.put(cacheKey, id);

        //4.返回id
        return id;
    }

    private int execSql(String[] sqls, Connection connection, BaseDimension baseDimension) throws SQLException {
        int id = -1;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sqls[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //第一次查询
        setArguments(preparedStatement,baseDimension);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);//*********************
        }
        //查询不到，插入数据
        preparedStatement = connection.prepareStatement(sqls[1]);
        setArguments(preparedStatement,baseDimension);
        preparedStatement.executeUpdate();

        //第二次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArguments(preparedStatement,baseDimension);
        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return id;
    }

    private void setArguments(PreparedStatement preparedStatement, BaseDimension baseDimension) throws SQLException {
        int i = 0;
        if (baseDimension instanceof ContactDimension){
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            preparedStatement.setString(++i,contactDimension.getPhoneNum());
            preparedStatement.setString(++i,contactDimension.getName());
        }else {
            DateDimension dateDimension = (DateDimension) baseDimension;
            preparedStatement.setInt(++i,Integer.valueOf(dateDimension.getYear()));
            preparedStatement.setInt(++i,Integer.valueOf(dateDimension.getMonth()));
            preparedStatement.setInt(++i,Integer.valueOf(dateDimension.getDay()));
        }
    }

    private String[] getSqls(BaseDimension baseDimension) {
        String[] sqls = new String[2];
        if (baseDimension instanceof ContactDimension){
            sqls[0]="select `id` from `tb_contacts` where `telephone`=? and `name`=?;";
            sqls[1] = "insert into `tb_contacts` values(NULL,?,?);";
        }else if (baseDimension instanceof DateDimension){
            sqls[0]="select `id` from `tb_dimension_date` where `year`=? and `month`=? and `day`=?;";
            sqls[1] = "insert into `tb_dimension_date` values(NULL,?,?,?);";
        }
        return sqls;
    }

    private String getCacheKey(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            sb.append(contactDimension.getPhoneNum());
        } else if (baseDimension instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) baseDimension;
            sb.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        }
        return sb.toString();
    }
}
