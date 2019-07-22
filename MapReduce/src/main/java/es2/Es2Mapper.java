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
            // Average number of episodes and average duration of anime grouped grouped by type
            String episodes = formattedLine[8];
            int duration = convertDuration(formattedLine[13]);
            String source = formattedLine[7];

            if (duration >= 0)
                context.write(new Text(source), new Text(episodes+","+duration));
        }
        catch (Exception e){
        }
    }

    /**
     * Data Duration for this case study
     * @param duration anime duration in hr,min or sec
     * @return converted value in minutes
     */
    private int convertDuration(String duration) {

        String[] parts = duration.replaceAll(" per ep.","").split(" ");
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
