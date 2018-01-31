package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Created by erzhang on 1/30/18.
 */
public class CommonFriendsTwo {

    public static class CommonFriendsTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String pair = splits[0];
            String friend = splits[1];
            k.set(pair);
            v.set(friend);
            context.write(k,v);
        }
    }

    public static class CommonFriendsTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text value:values){
                sb.append(value).append(",");
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job j = Job.getInstance(conf,"Step two");

        j.setJarByClass(CommonFriendsTwo.class);

        j.setMapperClass(CommonFriendsTwoMapper.class);
        j.setReducerClass(CommonFriendsTwoReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);

        j.setInputFormatClass(TextInputFormat.class);
        j.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(j, new Path("/Developer/DATA/hadoop/output1"));
        FileOutputFormat.setOutputPath(j, new Path("/Developer/DATA/hadoop/output2"));

        System.exit(j.waitForCompletion(true)? 0:1);


    }

}
