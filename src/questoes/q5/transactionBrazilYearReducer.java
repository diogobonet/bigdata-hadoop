package questoes.q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class transactionBrazilYearReducer extends Reducer<Text, averageWritable, Text, LongWritable> {
    // Reduce function
    public void reduce(Text key, Iterable<averageWritable> values, Context con) throws IOException, InterruptedException {
        Long average = this.calculateAverage(values);
        con.write(key, new LongWritable(average));
    }

    public Long calculateAverage(Iterable<averageWritable> values) {
        long quantity = 0;
        long totalSum = 0;

        for (averageWritable value : values){
            quantity += value.getQuantity();
            totalSum += value.getTotal();
        }

        return totalSum / quantity;
    }
}
