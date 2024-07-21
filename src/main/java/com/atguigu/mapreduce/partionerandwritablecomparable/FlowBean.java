package com.atguigu.mapreduce.partionerandwritablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义序列化Bean的一般流程
 * 1、自定义Bean类
 * 2、实现Writable接口
 * 3、重写序列化和反序列化方法（一定注意属性序列化和反序列的顺序必须一致）
 * 4、创建空参构造，因为反序列化的时候反射调用空参构造
 * 5、重写ToString方法
 * 6、如果该自定义Bean对象还要作为最终输出的Key，还必须实现Comparable接口，因为MapReduce会对Key进行默认的字典序排序，底层调用的Comparable接口的Comparator方法
 *
 * @author Hliang
 * @create 2022-10-11 17:33
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow(){
        this.sumFlow = this.upFlow + this.downFlow;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        // 二次排序，先按总流量从高到低排序
        if(this.sumFlow > o.sumFlow){
            return -1;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else {
            // 如果总流量相同，那么按照上行流量从低到高排序
            if(this.upFlow > o.upFlow){
                return 1;
            }else if(this.upFlow < o.upFlow){
                return -1;
            }else {
                return 0;
            }
        }
    }
}
