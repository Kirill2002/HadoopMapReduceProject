package lsds.project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task2Job1Reducer extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        int num= 0;

        for (Text value : values) {
            ++num;
        }

        context.write(key, new IntWritable(num));
    }
}

