package es2;

import es1.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Es2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line  = value.toString();

        String[] formattedLine = Utils.clearESplit(line);
        try {
            //Numero medio di episodi e durata media delle puntate degli anime raggruppati per tipologia
            String episodes = formattedLine[8];
            int duration = convertDuration(formattedLine[13]);
            String source = formattedLine[7];

            if (duration >= 0)
                context.write(new Text(source), new Text(episodes+","+duration));
        }
        catch (Exception e){
        }
    }

    private int convertDuration(String s) {

        String[] parts = s.replaceAll(" per ep.","").split(" ");
        if (parts.length > 1) {
            int result = 0;

            for (int i = 0; i < parts.length; i+=2) {
                String um = parts[i+1];
                if (um.equals("hr."))
                    result += Integer.parseInt(parts[i])*60;
                else if (um.equals("min."))
                    result += Integer.parseInt(parts[i]);
            }
            return result;
        }
        else return -1;
    }
}
