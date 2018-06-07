package com.atguigu.kv.key;

import com.atguigu.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 此类为了作为Mapper方法的参数类型
 * Created by MiYang on 2018/6/4.
 */
public class CommDimension extends BaseDimension {
    private ContactDimension contactDimension=new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    public CommDimension() {
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public int compareTo(BaseDimension o) {
        CommDimension other = (CommDimension) o;
        int result = this.contactDimension.compareTo(other.contactDimension);
        if (result==0){
            result = this.dateDimension.compareTo(other.dateDimension);
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.contactDimension.write(dataOutput);
        this.dateDimension.write(dataOutput);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.contactDimension.readFields(dataInput);
        this.dateDimension.readFields(dataInput);

    }
}
