package es1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Phase3Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String, Integer> counts = new HashMap<String, Integer>();
        for (Text usertitle :values) {
            String title = usertitle.toString();
            if(counts.containsKey(title)){
                counts.put(title,counts.get(title)+1);
            }
            else {
                counts.put(title,1);
            }
        }
        Iterator<Map.Entry<String, Integer>> iter = Utils.sortByValue(counts).entrySet().iterator();
        for (int i = 0 ; i<5 && iter.hasNext();i++){
            Map.Entry<String, Integer> couple = iter.next();
            context.write(key,new Text(couple.getKey() + "," + couple.getValue()));
        }
    }
}
