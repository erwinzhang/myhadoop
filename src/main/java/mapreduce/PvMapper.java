package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by erzhang on 1/26/18.
 */
public class PvMapper extends Mapper<LongWritable, Text, Text,IntWritable>{

    /***
     * MR framework will call this map method everytime when read a line
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String ip = line.split(" ")[0];

        context.write(new Text(ip), new IntWritable(1));
    }

    public static void main(String[] args) {

    }
}
