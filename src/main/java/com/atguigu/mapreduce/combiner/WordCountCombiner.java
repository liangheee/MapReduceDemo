package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * 其实Combiner就是一个局部的Reducer
 * Combiner和Reducer的不同之处主要就是在于运行所处的阶段不同
 * Combiner在MapTask节点上完成，Map结束后的环形缓冲区溢写磁盘之前执行以及溢写磁盘后的溢写后的文件的归并排序后的再次合并
 * 而Reducer则是在shuffle阶段之后进行全局的合并
 *
 * 但是Combiner和Reducer的代码一般来说是一样的
 *
 * @author Hliang
 * @create 2022-10-13 16:04
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key,outV);
    }
}
