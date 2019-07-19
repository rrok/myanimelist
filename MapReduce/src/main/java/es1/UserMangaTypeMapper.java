package es1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserMangaTypeMapper extends Mapper<Text,Text,Text,Text> {
    public static final char source = 'm';
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
       context.write(key,new Text(source+","+value));
    }
}
