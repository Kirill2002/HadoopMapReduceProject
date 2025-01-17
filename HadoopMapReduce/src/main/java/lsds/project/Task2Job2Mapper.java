package lsds.project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task2Job2Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final static int MOVIE_TITLE = 0;
    private final static int NUMBER_OF_LIKES = 1;

    public String[] extractTitleAndNumLikes(String line) {
        String[] fields = new String[2];
        String[] split = line.trim().split("\t");

        fields[NUMBER_OF_LIKES] = split[split.length - 1];
        fields[MOVIE_TITLE] = "";

        if (split.length < 2)
            return fields;

        for (int i = 0; i < split.length - 1; ++i)
        {
            fields[MOVIE_TITLE] += split[i];
        }

        return fields;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        if (key.get() == 0)
        {
            return;
        }

        String line = value.toString();
        String[] fields = extractTitleAndNumLikes(line);
        Text movieTitle = new Text(fields[MOVIE_TITLE]);
        IntWritable numLikes = new IntWritable(Integer.parseInt(fields[NUMBER_OF_LIKES]));

        context.write(numLikes, movieTitle);
    }
}

