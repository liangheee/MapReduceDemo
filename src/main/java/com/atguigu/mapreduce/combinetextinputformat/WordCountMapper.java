package com.atguigu.mapreduce.combinetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2022-10-11 16:27
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 拿到一行数据
        String line = value.toString();
        // 按文本中的规范进行分割
        String[] split = line.split(" ");
        for (String s : split) {
            outK.set(s);
            context.write(outK,outV);
        }
    }
}
