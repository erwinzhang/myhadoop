package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by erzhang on 1/28/18.
 */
public class WordIndexOne {

    public static class WordIndexOneMapper extends Mapper<LongWritable,IntWritable,Text,IntWritable>{

        String fileName;
        /***
         * Before map, setup will be called once
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            fileName = fs.getPath().getName();  // Get file split name

        }

        @Override
        protected void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            String line = key.toString();

            String[] words = line.split(" ");

            for(String word : words){
                context.write(new Text(word + "-" + fileName),new IntWritable(1));
            }

        }


        /**
         * After map, clearnup will call
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class WordIndexOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for(IntWritable value:values){
                count += value.get();
            }

            context.write(key, new IntWritable(count));

        }
    }



}
