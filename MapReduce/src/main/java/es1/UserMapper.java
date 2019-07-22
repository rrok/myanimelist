package es1;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserMapper extends Mapper<LongWritable, Text,Text,Text> {
    public static final char source = 'u';
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line  = value.toString();
        //remove all the string between " ".
        String[] formattedLine = Utils.clearESplit(line);
        try{
            int birthYear = Integer.parseInt(formattedLine[10].substring(0,4));
            int years = 2019 - birthYear;
            String zone;
            if(years < 25) zone = "0-24";
            else if (years < 50) zone = "25-49";
            else zone = "50-99";
            context.write(new Text(formattedLine[0]), new Text(source+","+zone));
        }catch (Exception e){}

    }
}
