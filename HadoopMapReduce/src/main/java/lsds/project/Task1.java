package lsds.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {
    private final static String ratingsPath = "ratings.csv";
    private final static String moviesPath = "movies.csv";
    private final static String JOB_1_NAME = "HighestRatedMovieIDPerUser";
    private final static String JOB_2_NAME = "HighestRatedMoviePerUser";

    public static void main(String[] args) throws Exception  {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path inputRatings = new Path(ratingsPath);
        Path inputMovies = new Path(moviesPath);
        Path outputJob1 = new Path(files[0] + "_" + JOB_1_NAME);
        FileSystem fs = FileSystem.get(c);
        if(fs.exists(outputJob1)){
            fs.delete(outputJob1, true);
        }
        Job job1 = new Job(c, JOB_1_NAME);
        job1.setJarByClass(Task1.class);
        job1.setMapperClass(Task1Job1Mapper.class);
        job1.setReducerClass(Task1Job1Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, inputRatings);
        FileOutputFormat.setOutputPath(job1, outputJob1);
        job1.waitForCompletion(true);

        Path outputJob2 = new Path(files[0] + "_" + JOB_2_NAME);

        if(fs.exists(outputJob2)){
            fs.delete(outputJob2, true);
        }
        Job job2 = new Job(c, JOB_2_NAME);
        job2.setJarByClass(Task1.class);
//        j2.setMapperClass(Task1Job1Mapper.class);
        job2.setReducerClass(Task1Job2Reducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job2, outputJob1, TextInputFormat.class, Task1Job2MapperMovieIdPerUser.class);
        MultipleInputs.addInputPath(job2, inputMovies, TextInputFormat.class, Task1Job2MapperMovie.class);
        FileOutputFormat.setOutputPath(job2, outputJob2);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}