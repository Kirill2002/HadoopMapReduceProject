package lsds.project;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task1Job2MapperMovie extends Mapper<LongWritable, Text, Text, Text> {
    private final static int MOVIE_ID = 0;
    private final static int TITLE = 1;
    private final static int MAPPER_TYPE = 0;

    public String[] extractMovieIdAndTitle(String line) {
        String[] fields = new String[2];
        String[] split = line.split(",");

        fields[MOVIE_ID] = split[MOVIE_ID];
        fields[TITLE] = "";

        if (split.length < 2)
            return fields;

        for (int i = 1; i < split.length - 1; ++i)
        {
            fields[TITLE] += split[i];
        }

        return fields;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
        if (key.get() == 0)
        {
            return;
        }

        String line = value.toString();
        String[] fields = extractMovieIdAndTitle(line.trim());

        Text movieId = new Text(fields[MOVIE_ID]);
        Text resValue = new Text(MAPPER_TYPE + "," + fields[TITLE]);

        context.write(movieId, resValue);
    }
}

