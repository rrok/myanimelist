package es1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MangaUsersMapper extends Mapper<Text,Text,Text,Text> {
    public static final char source = 'm';
    @Override
    protected void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        for (String username: value.toString().split(",")
             ) {
            context.write(new Text(username),key);
        }
    }
}
