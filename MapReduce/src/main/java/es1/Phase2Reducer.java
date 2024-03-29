package es1;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Phase2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String zone = null ;
        List<String> midle = new ArrayList<String>();
        for (Text value:values) {
            //
            if(value.toString().charAt(0)== MangaUsersMapper.source){
                midle.add(value.toString().substring(2));
            }
            else
                zone= value.toString().substring(2);
        }
        // if it happens that a username does not exist (it has been cleaned by who created the csv) we don't consider the anime list record
        if(zone!=null)
            for (String titleSource: midle) {
                String[] titleSourceSplitted= titleSource.split(",");
                context.write(new Text(zone+","+titleSourceSplitted[1]),new Text(titleSourceSplitted[0]));
            }
    }
}
