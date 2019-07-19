package es2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Es2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float avgDuration = 0;
        float avgEpisodes = 0;

        int count = 0;

        for (Text episodesDuration: values) {
            String[] parts = episodesDuration.toString().split(",");
            avgDuration += Integer.parseInt(parts[0]);
            avgEpisodes += Integer.parseInt(parts[1]);

            count++;
        }

        avgDuration /= count;
        avgEpisodes /= count;

        context.write(key, new Text(avgEpisodes + " episodes, " + avgDuration + " minutes"));
    }
}