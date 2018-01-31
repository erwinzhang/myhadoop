package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by erzhang on 1/30/18.
 */
public class CommonFriendsOne {

    public static class CommonFriendsOneMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A:B,C,D,F
            String line = value.toString();
            String[] lineSplit = line.split(":");
            String name = lineSplit[0];
            String[] friends = lineSplit[1].split(",");
            v.set(name);
            for(String friend : friends){
                k.set(friend);
                context.write(k,v);
            }
            //output:   B A     C A     D A     F A
        }
    }

    public static class CommonFriendsOneReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text friend, Iterable<Text> users, Context context) throws IOException, InterruptedException {
            //Input  B A        B C     B D ...

            ArrayList<Text> usersList = new ArrayList<Text>();

            for(Text user: users){
                usersList.add(new Text(user));
            }

            Collections.sort(usersList);

            for(int i = 0; i < usersList.size()-1; i++){
                for(int j = i+1; j < usersList.size(); j++){
                    context.write(new Text(usersList.get(i) + "-" + usersList.get(j)),friend);
                }
            }
            //Output A-C B  A-D B  C-D B
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println(System.getProperty("java.class.path"));
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framework.name","yarn");

        Job j = Job.getInstance(conf,"Step one");

        j.setJarByClass(CommonFriendsOne.class);

        j.setMapperClass(CommonFriendsOneMapper.class);
        j.setReducerClass(CommonFriendsOneReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);

//        j.setOutputKeyClass(Text.class);
//        j.setOutputValueClass(Text.class);

        j.setInputFormatClass(TextInputFormat.class);
        j.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(j, new Path("/Developer/DATA/hadoop/input/"));
        FileOutputFormat.setOutputPath(j, new Path("/Developer/DATA/hadoop/output1"));

        System.exit(j.waitForCompletion(true)? 0:1);


    }



}
