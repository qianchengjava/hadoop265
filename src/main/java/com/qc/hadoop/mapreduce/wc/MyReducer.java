package com.qc.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *        Reducer<Text, IntWritable, Text, IntWritable>
 *              * hello    1          hello  3
 *              * hello    1          world  1
 *              * world    1          qian   1
 *              * qian     1          cheng  1
 *              * cheng    1
 *              * hello    1
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    //相同的key为一组 ，这一组数据调用一次reduce
    //hello 1
    //hello 1
    //hello 1
    //hello 1
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,/* 111111*/
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
