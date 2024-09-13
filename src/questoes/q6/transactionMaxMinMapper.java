package questoes.q6;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class transactionMaxMinMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    // Função utilizada para o Mapeamento dos dados.
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String columnHeadName = "country_or_area";
        Boolean isColumnHead = line.startsWith(columnHeadName);
        String[] columnsArray = line.split(";"); // Transformar/dividr os valores da linha atual em Array.

        String countryValue = columnsArray[0];
        Boolean isNotBrazilData = !countryValue.equals("Brazil");

        if (isColumnHead || isNotBrazilData) return;

        String yearValue = columnsArray[1];

        if (yearValue.equals("2016")) {
            Text outputKey = new Text(columnsArray[0]);

            con.write(outputKey, new LongWritable(Long.parseLong(columnsArray[5])));
        }
    }
}
