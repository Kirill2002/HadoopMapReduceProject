package lsds.project;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.InterruptedException;

public class Task1Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static int USER_ID = 0;
    private final static int MOVIE_ID = 1;
    private final static int SCORE = 2;

    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        if (key.get() == 0)
        {
            return;
        }

        String line = value.toString();
        String[] fields = line.trim().split(",");
        String return_value = fields[MOVIE_ID] + "," + fields[SCORE];
        Text userId = new Text(fields[USER_ID]);

        context.write(userId, new Text(return_value));
    }
}

