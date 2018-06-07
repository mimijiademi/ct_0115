package com.atguigu.convertor;

import com.atguigu.kv.base.BaseDimension;

/**
 * 转化接口，用于根据传入的维度对象，得到该维度
 * Created by MiYang on 2018/6/5 10:28.
 */
public interface IConvertor {
    // 根据传入的baseDimension对象，获取数据库中对应该对象数据的id，如果不存在，则插入该数据再返回
    int getDimensionID(BaseDimension baseDimension);
}
