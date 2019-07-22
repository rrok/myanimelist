package es2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main2 {
    public static void main(String[] args) throws Exception {


        Job job = new Job(new Configuration(), "Mean number of episodes and duration");
        job.setJarByClass(Main2.class);

        job.setMapperClass(Es2Mapper.class);
        job.setReducerClass(Es2Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
