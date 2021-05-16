package com.qc.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Collection;
import java.util.List;

/**
 * com.qc.hadoop.mapreduce.wc.MyWordCount
 */
public class MyWordCount {


    //bin/hadoop command [genericOptions] [commandOptions]
    //    hadoop jar  ooxx.jar  ooxx   -D  ooxx=ooxx  inpath  outpath
    //  args :   2类参数  genericOptions   commandOptions
    //  人你有复杂度：  自己分析 args数组
    //
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        //工具类帮我们把-D 等等的属性直接set到conf，会留下commandOptions
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] othargs = parser.getRemainingArgs();

        //让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform", "true");

        //conf.set("mapreduce.framework.name","local");
        //System.out.println(conf.get("mapreduce.framework.name"));
        Job job = Job.getInstance(conf);
        FileInputFormat.setMinInputSplitSize(job, 2);
        // job.setInputFormatClass(ooxx.class);
        //job.setJar("C:\\Users\\admin\\IdeaProjects\\msbhadoop\\target\\hadoop-hdfs-1.0-0.1.jar");
        //必须必须写的
        job.setJarByClass(MyWordCount.class);

        String jobName = othargs[2];
        job.setJobName(jobName);
        //job.setJobName("qiancheng_wc_job1");

        Path infile = new Path(othargs[0]);
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path(othargs[1]);
        if (outfile.getFileSystem(conf).exists(outfile)) outfile.getFileSystem(conf).delete(outfile, true);
        TextOutputFormat.setOutputPath(job, outfile);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }

}
