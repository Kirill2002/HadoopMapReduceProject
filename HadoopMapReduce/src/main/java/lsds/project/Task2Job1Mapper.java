package lsds.project;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task2Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static int USER_ID = 0;
    private final static int MOVIE_TITLE = 1;

    public String[] extractUserIdAndTitle(String line) {
        String[] fields = new String[2];
        String[] split = line.trim().split("\t");

        fields[USER_ID] = split[USER_ID];
        fields[MOVIE_TITLE] = "";

        if (split.length < 2)
            return fields;

        for (int i = 1; i < split.length; ++i)
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
        String[] fields = extractUserIdAndTitle(line);
        Text movieTitle = new Text(fields[MOVIE_TITLE]);

        context.write(movieTitle, new Text("1"));
    }
}

