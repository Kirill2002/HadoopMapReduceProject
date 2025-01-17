package lsds.project;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.InterruptedException;

public class Task1Job1Reducer extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        Double highestRating = 0.0;
        int bestMovieId = -1;

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            Integer movieId = Integer.parseInt(fields[0]);
            Double rating = Double.parseDouble(fields[1]);
            if (highestRating < rating){
                highestRating = rating;
                bestMovieId = movieId;
            }

        }

        context.write(new Text(key.toString()), new IntWritable(bestMovieId));
    }
}

