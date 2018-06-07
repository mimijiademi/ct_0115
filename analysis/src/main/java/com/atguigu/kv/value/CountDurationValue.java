package com.atguigu.kv.value;

import com.atguigu.kv.base.BaseValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by MiYang on 2018/6/4.
 */
public class CountDurationValue extends BaseValue {
    private String countSum;
    private String durationSum;

    public CountDurationValue() {
    }

    public String getCountSum() {
        return countSum;
    }

    public void setCountSum(String countSum) {
        this.countSum = countSum;
    }

    public String getDurationSum() {
        return durationSum;
    }

    public void setDurationSum(String durationSum) {
        this.durationSum = durationSum;
    }

    @Override
    public String toString() {
        return countSum + "\t" + durationSum;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(countSum);
        dataOutput.writeUTF(durationSum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.countSum = dataInput.readUTF();
        this.durationSum = dataInput.readUTF();

    }
}
