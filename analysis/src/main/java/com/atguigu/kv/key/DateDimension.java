package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 时间维度
 * Created by MiYang on 2018/6/4.
 */
public class DateDimension extends BaseDimension {
    //时间维度：当前通话信息所在年
    private String year;
    //时间维度：当前通话信息所在月,如果按照年来统计信息，则month为-1
    private String month;
    //时间维度：当前通话信息所在日,如果按照年来统计信息，则day为-1。
    private String day;

    public DateDimension() {
    }

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return year + "\t" + month + "\t" + day;
    }


    public int compareTo(BaseDimension o) {
        DateDimension other = (DateDimension) o;
        //先判断年是否相等
        int result = this.year.compareTo(other.year);
        //判断月是否相等
        if (result==0){
            result = this.month.compareTo(other.month);
        }
        //判断天是否相等
        if (result==0){
            result = this.day.compareTo(other.day);
        }
        return result;
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(day);

    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();

    }
}
