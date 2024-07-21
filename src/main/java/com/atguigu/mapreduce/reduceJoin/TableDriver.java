package com.atguigu.mapreduce.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2022-10-14 15:38
 */
public class TableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1、创建Job实例对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置当前驱动类所在的Jar包路径
        job.setJarByClass(TableDriver.class);

        // 3、关联Mapper和Reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        // 4、设置Mapper的输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5、设置最终输出的KV类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6、设置输出数据和输出数据的路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputtable"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\output12"));

        // 7、提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
