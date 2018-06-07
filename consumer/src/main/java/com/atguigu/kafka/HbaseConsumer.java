package com.atguigu.kafka;

import com.atguigu.dao.HbaseDAO;
import com.atguigu.utils.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;

/**
 * Created by MiYang on 2018/6/1.
 */
public class HbaseConsumer {
    public static void main(String[] args) throws Exception {
        //获取kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(PropertyUtil.properties);
        kafkaConsumer.subscribe(Collections.singletonList(PropertyUtil.getProperty("kafka.topic")));
        //创建HbaseDAO对象（作用：写入数据）
        HbaseDAO hbaseDAO = new HbaseDAO();

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(300);
            for (ConsumerRecord<String, String> record : records) {
                String ori = record.value();
                System.out.println(ori);
                hbaseDAO.put(ori);
            }
        }
    }
}

