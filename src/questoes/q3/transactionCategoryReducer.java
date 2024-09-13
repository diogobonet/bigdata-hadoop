package questoes.q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class transactionCategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Funcao de reduce
    public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
        int occurrences = this.countOccurrences(values);
        con.write(key, new IntWritable(occurrences));
    }

    public int countOccurrences(Iterable<IntWritable> values) {
        int totalSum = 0;

        for (IntWritable value : values) {
            totalSum += value.get();
        }

        return totalSum;
    }
}
