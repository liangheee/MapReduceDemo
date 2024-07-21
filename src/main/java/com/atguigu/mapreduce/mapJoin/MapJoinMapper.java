package com.atguigu.mapreduce.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author Hliang
 * @create 2022-10-14 16:30
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private HashMap<String,String> pdMap = new HashMap<>();
    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取到缓存文件所在的路径
        URI[] cacheFiles = context.getCacheFiles();

        // 开始读取缓存文件
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFiles[0]));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream,"UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] split = line.split("\t");
            pdMap.put(split[0],split[1]);
        }
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("\t");

        outK.set(split[0] + " \t" + pdMap.get(split[1]) + "\t" + split[2]);
        context.write(outK,NullWritable.get());

    }

}
