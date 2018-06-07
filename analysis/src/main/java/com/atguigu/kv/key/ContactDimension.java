package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * call电话号码维度
 * Created by MiYang on 2018/6/4.
 */
public class ContactDimension extends BaseDimension {
    private String name;
    private String phoneNum;

    public ContactDimension() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public String toString() {
        return name + "\t" + phoneNum;
    }

    //比较联系人是否是同一个
    public int compareTo(BaseDimension o) {
        ContactDimension other = (ContactDimension) o;
        return phoneNum.compareTo(other.phoneNum);
    }

    //序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(phoneNum);
    }

    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.phoneNum = dataInput.readUTF();

    }
}
