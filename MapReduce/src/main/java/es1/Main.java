package es1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) throws Exception {

        //hadoop jar MapReduce.jar es1.Main anime_cleaned.csv animelists_cleaned.csv users_cleaned.csv ./out

        List<Job> jobs = new ArrayList<Job>();
        jobs.add(Job.getInstance(new Configuration(),"join anime with animeList"));
        jobs.add(Job.getInstance(new Configuration(),"join result with user"));
        jobs.add(Job.getInstance(new Configuration(),"Group by Zone Source"));


        //il job deve per partire questa classe
        jobs.get(0).setJarByClass(Main.class);
        jobs.get(1).setJarByClass(Main.class);
        jobs.get(2).setJarByClass(Main.class);

        MultipleInputs.addInputPath(jobs.get(0), new Path(args[0]), TextInputFormat.class,AnimeMapper.class);
        MultipleInputs.addInputPath(jobs.get(0), new Path(args[1]), TextInputFormat.class,AnimeListMapper.class);

        //version 1
        //jobs.get(0).setReducerClass(Phase1Reducer.class);

        //version 2 optimized
        jobs.get(0).setReducerClass(Phase1ReducerOptimized.class);

        FileOutputFormat.setOutputPath(jobs.get(0), new Path(args[3]+"/middle1"));

        jobs.get(0).setOutputKeyClass(IntWritable.class);
        jobs.get(0).setOutputValueClass(Text.class);
        jobs.get(0).waitForCompletion(true);

        //new Path(args[3]) cioè l'output del primo diventa l'imput del secondo job
        MultipleInputs.addInputPath(jobs.get(1), new Path(args[3]+"/middle1/*"), KeyValueTextInputFormat.class,UserMangaTypeMapper.class);
        MultipleInputs.addInputPath(jobs.get(1), new Path(args[2]), TextInputFormat.class,UserMapper.class);

        jobs.get(1).setReducerClass(Phase2Reducer.class);
        FileOutputFormat.setOutputPath(jobs.get(1), new Path(args[3]+"/middle2"));

        jobs.get(1).setOutputKeyClass(Text.class);
        jobs.get(1).setOutputValueClass(Text.class);
        jobs.get(1).waitForCompletion(true);

        jobs.get(2).setMapperClass(Phase3Mapper.class);
        jobs.get(2).setReducerClass(Phase3Reducer.class);
        jobs.get(2).setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(jobs.get(2),new Path(args[3]+"/middle2/*"));
        FileOutputFormat.setOutputPath(jobs.get(2),new Path(args[3]+"/Final"));


        jobs.get(2).setOutputKeyClass(Text.class);
        jobs.get(2).setOutputValueClass(Text.class);
        jobs.get(2).waitForCompletion(true);





    }
}
