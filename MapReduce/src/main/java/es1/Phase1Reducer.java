package es1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Phase1Reducer extends Reducer<IntWritable, Text,Text,Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //la kay è l'id di tutti i record impostati nei mapper

        //in anime ci metterò titolo e source
        String anime = null ;

        //in questa lista fornita dall'altro mapper inserisco i nomi dei tizi che hanno letto questo anime
        List<String> animeListRecords = new ArrayList<String>();


        //scorro e capisco dalla source la provenienza e salvo i dati.
        for (Text value:values) {
            //
            if(value.toString().charAt(0)== AnimeListMapper.source){
                //tolgo la sorgente perché non mi serve più
                animeListRecords.add(value.toString().substring(2));
            }
            else {
                anime= value.toString().substring(2);
            }
        }
        //se capita che un username non esiste(è stato pulito da chi ha creato il csv) non considero l'anime list record
        if(anime!=null)
        for (String animeListRecord: animeListRecords) {
            //scrivo
            context.write(new Text(animeListRecord),new Text(anime));
        }

    }
}