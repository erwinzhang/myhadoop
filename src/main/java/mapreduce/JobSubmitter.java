package mapreduce;

/**
 * Created by erzhang on 1/26/18.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/***
 * YARN client
 * The feature is submit mapreduce jar package to yarn, yarn will allocate jar to nodemanager to execute
 */
public class JobSubmitter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job j = Job.getInstance(conf);

        j.setJar("/root/myhadoop.jar");
        j.setMapperClass(PvMapper.class);
        j.setReducerClass(PvReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        j.setInputFormatClass(TextInputFormat.class);
        JobConf jobConf = new JobConf();

        FileInputFormat.setInputPaths(jobConf, new Path("/"));

        FileOutputFormat.setOutputPath(jobConf, new Path("/out.txt"));
        j.setPartitionerClass(ProvincePartioner.class);
        j.setNumReduceTasks(5);

        boolean res = j.waitForCompletion(true);
        System.exit(res? 0:1);

        //How to run: hadoop jar myhadoop.jar, it will set classpath automatically

    }


}
