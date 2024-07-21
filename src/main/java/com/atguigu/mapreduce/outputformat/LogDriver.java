package com.atguigu.mapreduce.outputformat;

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
 * @create 2022-10-13 16:08
 */
public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1、创建Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置驱动类的Jar包所在路径
        job.setJarByClass(LogDriver.class);

        job.setOutputFormatClass(LogOutputFormat.class);

        // 3、绑定Mapper和Reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        // 4、设置Mapper的输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5、设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6、设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputoutputformat"));
        // 虽然我们自定义的LogOutputFormat里面的Record已经设置了输出流，但是由于FileOutputFormat需要
        // 输出_success文件，所以必须在这里还要设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\output11"));

        // 7、提交作业
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0: 1);

    }

}
