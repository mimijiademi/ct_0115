package com.atguigu.mr;

import com.atguigu.convertor.DimensionConvertorImpl;
import com.atguigu.kv.base.BaseDimension;
import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.value.CountDurationValue;
import com.atguigu.util.JDBCInstance;
import com.atguigu.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Created by MiYang on 2018/6/5 9:28.
 */
public class MySQLOutPutFormat extends OutputFormat<BaseDimension, CountDurationValue> {

    private FileOutputCommitter committer = null;

    public RecordWriter<BaseDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //创建jdbc连接
        Connection connection = null;
        try {
            connection = JDBCInstance.getInstance();
            //关闭自动提交，以便于批量提交
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new MysqlRecordWriter(connection);
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // 校检输出
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    private static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    static class MysqlRecordWriter extends RecordWriter<BaseDimension, CountDurationValue> {
        private Connection connection = null;
        private PreparedStatement preparedStatement = null;
        private int batchBound = 500;//缓存sql条数边界上限
        private int batchSize = 0;//客户端已经缓存的条数

        public MysqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        public void write(BaseDimension baseDimension, CountDurationValue value) throws IOException, InterruptedException {
            CommDimension commDimension = (CommDimension) baseDimension;
            //统计当前PreparedStatement对象待提交的数据量,插入数据到mysql
            String sql = "insert into ct.tb_call values(?,?,?,?,?) on duplicate key update `call_sum`=?,`call_duration_sum`=?;";
            //维度转换
            DimensionConvertorImpl convertor = new DimensionConvertorImpl();

            int dateId = convertor.getDimensionID(commDimension.getDateDimension());
            int contactId = convertor.getDimensionID(commDimension.getContactDimension());

            String date_contact = dateId + "_" + contactId;

            int countSum = Integer.valueOf(value.getCountSum());
            int durationSum = Integer.valueOf(value.getDurationSum());

            try {
                if (preparedStatement == null) {
                    preparedStatement = connection.prepareStatement(sql);
                }
                // 本次sql
                int i = 0;
                preparedStatement.setString(++i, date_contact);
                preparedStatement.setInt(++i, dateId);
                preparedStatement.setInt(++i, contactId);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);

                //将sql缓存到客户端
                preparedStatement.addBatch();
                //当前缓存了多少个sql语句等待批量执行，计数器
                batchSize++;
                // 批量提交
                if (batchSize >= batchBound) {
                    //批量执行sql
                    preparedStatement.executeBatch();
                    // 连接提交
                    connection.commit();
                    //清零
                    batchSize = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                if (preparedStatement != null) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
                JDBCUtil.close(connection, preparedStatement, null);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

