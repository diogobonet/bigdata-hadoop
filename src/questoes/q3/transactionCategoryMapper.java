package questoes.q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class transactionCategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Função utilizada para o Mapeamento dos dados.
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String columnHeadName = "country_or_area";
        Boolean isColumnHead = line.startsWith(columnHeadName);

        if (isColumnHead) return;

        String[] columnsArray = line.split(";"); // Transformar/dividr os valores da linha atual em Array.
        String categoryValue = columnsArray[9];


        Text outputKey = new Text(categoryValue);
        IntWritable amount = new IntWritable(1);

        con.write(outputKey, amount);
    }
}
