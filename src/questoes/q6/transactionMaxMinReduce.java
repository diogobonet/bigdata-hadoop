package questoes.q6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class transactionMaxMinReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    // Reduce function
    public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {

        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;

        for (LongWritable v : values) {
            long value = v.get();
            max = Math.max(max, value);
            min = Math.min(min, value);
        }

        // Emite o valor máximo
        con.write(key, new LongWritable(max));

        // Emite o valor mínimo
        con.write(key, new LongWritable(min));
    }

}
