package es1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Phase1ReducerOptimized extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //key is the record record provided by mapper
        //anime String will contain title and manga source
        String anime = null ;

        //animeListRecords will contain all username that read this anime
        List<String> animeListRecords = new ArrayList<String>();

        //check all values to know from whitch mapper are from
        for (Text value:values) {
            //
            if(value.toString().charAt(0)== AnimeListMapper.source){
                //clear data removing first source char and comma
                animeListRecords.add(value.toString().substring(2));
            }
            else {
                anime= value.toString().substring(2);
            }
        }
        // if it happens that a username does not exist (it has been cleaned by who created the csv) we don't consider the anime list record
        if(anime!=null){
            StringBuilder users = new StringBuilder();
            for (String user: animeListRecords) {
                //appends takes considerably less time than users + = "," + user;
                users.append(',').append(user);
            }
            context.write(new Text(anime),new Text(users.substring(1)));
        }
    }
}
