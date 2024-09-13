package questoes.q8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class transactionMaxMinAmountReduce extends Reducer<keyPairWritable, LongWritable, Text, Text> {
    // Reduce function
    public void reduce(keyPairWritable key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {

        long max = 0;
        long min = 1000000000;

        for (LongWritable v:values){
            if (v.get() > max){
                max = v.get();
            } else if (min > v.get()){
                min = v.get();
            }
        }

        String maxStr = String.valueOf(max);
        String minStr = String.valueOf(min);

        // Usando String.format para criar a string formatada
        String result = String.format("Max: %s | Min: %s", maxStr, minStr);
        String keyResult= String.format("País: %s | Ano: %s | ", key.getCountry(), key.getYear());

        // Escrevendo a saída
        con.write(new Text(keyResult), new Text(result));

    }
}
