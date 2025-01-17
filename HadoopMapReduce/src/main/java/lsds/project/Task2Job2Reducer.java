package lsds.project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task2Job2Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        StringBuilder moviesList = new StringBuilder();

        for (Text value : values) {
            if (moviesList.length() > 0) {
                moviesList.append(", ");
            }
            moviesList.append(value.toString());
        }


        context.write(key, new Text(moviesList.toString()));
    }
}

