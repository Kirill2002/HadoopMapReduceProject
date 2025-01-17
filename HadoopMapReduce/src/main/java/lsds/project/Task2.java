package lsds.project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {
    private final static String JOB_1_NAME = "NumberOfLikesPerMovie";
    private final static String JOB_2_NAME = "MoviePerNumberOfLikes";

    public static void main(String[] args) throws Exception  {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path outputJob1 = new Path(files[1] + "_" + JOB_1_NAME);
        FileSystem fs = FileSystem.get(c);
        if(fs.exists(outputJob1)){
            fs.delete(outputJob1, true);
        }
        Job job1 = new Job(c, JOB_1_NAME);
        job1.setJarByClass(Task2.class);
        job1.setMapperClass(Task2Job1Mapper.class);
        job1.setReducerClass(Task2Job1Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, outputJob1);
        job1.waitForCompletion(true);

        Path outputJob2 = new Path(files[1] + "_" + JOB_2_NAME);

        if(fs.exists(outputJob2)){
            fs.delete(outputJob2, true);
        }
        Job job2 = new Job(c, JOB_2_NAME);
        job2.setJarByClass(Task2.class);
        job2.setMapperClass(Task2Job2Mapper.class);
        job2.setReducerClass(Task2Job2Reducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, outputJob1);
        FileOutputFormat.setOutputPath(job2, outputJob2);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}