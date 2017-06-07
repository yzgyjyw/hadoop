package com.njust.learninghadoop.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2017/6/2.
 */

public class WordCount{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        /*configuration.set("mapreduce.framework.name","yarn");
        configuration.set("yarn.resourcemanager.hostname","bigdata02");
        configuration.set("fs.defaultFS","hdfs://bigdata02:9000");*/
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCount.class);

        /*job.setJar("G:\\Users\\Administrator\\IdeaProjects\\learninghadoop\\target\\learninghadoop-1.0-SNAPSHOT.jar");*/

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/input/"));

        FileOutputFormat.setOutputPath(job,new Path("/output2"));

        job.waitForCompletion(true);

    }
}

class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        for(String s:splits){
            context.write(new Text(s),new LongWritable(1L));
        }
    }
}

class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0L;
        for(LongWritable l:values){
            count = count+l.get();
        }
        context.write(key,new LongWritable(count));
    }
}

