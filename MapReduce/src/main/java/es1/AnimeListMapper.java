package es1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnimeListMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    public static final char source = 'l';

    protected void map(LongWritable key, Text value, Context context) {

        String line  = value.toString();

        // remove all the string between " ".
        String[] formattedLine = Utils.clearESplit(line);
        int id= -1;
        try {
            id= Integer.parseInt(formattedLine[1]);
            //l'id è anime_id
            //il valore corrispondente è la provenienza e il nome di chi l'ha letto
            context.write(new IntWritable(id), new Text(source+","+formattedLine[0]));
        }
        catch (Exception e){
        }
    }
}
