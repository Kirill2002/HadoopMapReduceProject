package lsds.project;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task1Job2MapperMovieIdPerUser extends Mapper<LongWritable, Text, Text, Text> {

    private final static int USER_ID = 0;
    private final static int MOVIE_ID = 1;
    private final static int MAPPER_TYPE = 1;

    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        if (key.get() == 0)
        {
            return;
        }

        String line = value.toString();
        String[] fields = line.trim().split("\t");

        Text movieId = new Text(fields[MOVIE_ID]);
        Text resValue = new Text(MAPPER_TYPE + "," + fields[USER_ID]);

        context.write(movieId, resValue);
    }
}

