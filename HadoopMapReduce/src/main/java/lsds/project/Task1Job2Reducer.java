package lsds.project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task1Job2Reducer extends Reducer<Text, Text, Text, Text> {
    private final static int MAPPER_TYPE_TITLE = 0;
    private final static int MAPPER_TYPE_MOVIE_PER_USER = 1;
    private final static Integer MAPPER_TYPE = 0;
    private final static Integer MOVIE_TITLE = 1;
    private final static Integer USER_ID = 1;


    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        String title = "";

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            Integer mapperType = Integer.parseInt(fields[MAPPER_TYPE]);

            if (mapperType == MAPPER_TYPE_TITLE){
                title = fields[MOVIE_TITLE];
                break;
            }
        }

        Text retValue = new Text(title);

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            Integer mapperType = Integer.parseInt(fields[MAPPER_TYPE]);

            if (mapperType == MAPPER_TYPE_MOVIE_PER_USER){
                Text userId = new Text(fields[USER_ID]);
                context.write(userId, retValue);
            }
        }
    }
}

