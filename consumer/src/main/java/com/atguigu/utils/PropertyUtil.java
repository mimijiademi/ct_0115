package com.atguigu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by MiYang on 2018/6/1.
 */
public class PropertyUtil {
    public static Properties properties;

    static {
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("kafka_hbase.properties");
        properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
