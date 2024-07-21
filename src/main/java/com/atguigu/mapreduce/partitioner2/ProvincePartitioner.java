package com.atguigu.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import com.atguigu.mapreduce.partitioner2.FlowBean;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Hliang
 * @create 2022-10-12 17:35
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        String phone = text.toString();

        int partition;
        String prePhone = phone.substring(0,3);

        if("136".equals(prePhone)){
            partition = 0;
        }else if("137".equals(prePhone)){
            partition = 1;
        }else if("138".equals(prePhone)){
            partition = 2;
        }else if("139".equals(prePhone)){
            partition = 3;
        }else {
            partition = 4;
        }

        return partition;
    }
}
