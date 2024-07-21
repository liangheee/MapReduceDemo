package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2022-10-11 16:33
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1、获取Job任务对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、获取当前Driver所在的Jar包路径
        job.setJarByClass(WordCountDriver.class);

        job.setCombinerClass(WordCountCombiner.class);

        // 3、绑定Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、设置Mapper的输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入数据路径和输出数据路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputword"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\output10"));

        // 7、提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
