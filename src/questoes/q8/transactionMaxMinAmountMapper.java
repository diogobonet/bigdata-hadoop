package questoes.q8;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class transactionMaxMinAmountMapper extends Mapper<LongWritable, Text, keyPairWritable, LongWritable> {
    // Função utilizada para o Mapeamento dos dados.
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String columnHeadName = "country";
        Boolean isColumnHead = line.startsWith(columnHeadName);
        String[] columnsArray = line.split(";"); // Transformar/dividr os valores da linha atual em Array.


        if (isColumnHead) return;

        String countryValue = columnsArray[0];
        String yearValue = columnsArray[1];
        keyPairWritable keyPair = new keyPairWritable(yearValue, countryValue);

        LongWritable tradeValue = new LongWritable(Long.parseLong(columnsArray[5]));
        con.write(keyPair, tradeValue);
    }
}
