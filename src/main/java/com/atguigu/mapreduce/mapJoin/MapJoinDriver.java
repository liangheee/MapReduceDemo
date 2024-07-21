package com.atguigu.mapreduce.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Hliang
 * @create 2022-10-14 16:31
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        // 1、获取Job任务实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置当前驱动类所在的Jar包
        job.setJarByClass(MapJoinDriver.class);

        // 3、绑定缓存文件，供Map阶段获取,注意URI格式！！！
        // 协议:///盘符  ===》这里不是文件路径
        // 本地的话就是file协议+磁盘路径，不过注意不是反斜杠
        // hdfs的话就是hdfs协议+路径
        job.addCacheFile(new URI("file:///D:/input/tablecache/pd.txt"));

        // 4、由于本案例在Map中进行Join，所以不需要Reduce阶段，所以设置reduceTask为0个，当然也可以不设置，
        // 因为不关联reduce，就不会走reduce阶段
        job.setNumReduceTasks(0);

        // 5、关联mapper
        job.setMapperClass(MapJoinMapper.class);

        // 6、设置Map阶段输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 7、设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 8、设置输入数据路径和输出数据路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\output13"));

        // 9、提交Job任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
