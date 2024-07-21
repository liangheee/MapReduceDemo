package com.atguigu.mapreduce.partionerandwritablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2022-10-11 17:56
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
