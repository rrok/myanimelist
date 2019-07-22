package es1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnimeMapper extends Mapper<LongWritable,Text, IntWritable, Text> {
    public static final char source = 'a';
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line  = value.toString();
        int id = -1;

        // remove all the string between " ".
        String[] formattedLine = Utils.clearESplit(line);
        try {
            id= Integer.parseInt(formattedLine[0]);
            //anime_id is used as key
            //id is the key and the vaule is composet starting with source followed by title and type(source) separated by a comma
            context.write(new IntWritable(id), new Text(source+","+formattedLine[1]+","+formattedLine[7]));
        }
        catch (Exception e){
        }

    }
}
