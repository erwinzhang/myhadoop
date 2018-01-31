package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by erzhang on 1/29/18.
 */
public class WordIndexTwo {

    public static class WordIndexTwoMapper extends Mapper<LongWritable,Text,Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] split = line.split("-");
            String word = split[0];

            String filename = split[1].split("\t")[0];
            String count = split[1].split("\t")[1];
            context.write(new Text(word),new Text(filename = "--" + count));

        }
    }


    public static class WordIndexTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();

            for(Text value : values){
                sb.append(value).append(" ");
            }

            context.write(key,new Text(sb.toString()));
        }
    }


}
