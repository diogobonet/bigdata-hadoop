package q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

//Número de transações envolvendo o Brasil.
public class transactionBrazil {
    public static void main(String[] args) throws Exception  {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] file = new GenericOptionsParser(c, args).getRemainingArgs();

        // Job creation
        Job job = Job.getInstance(c, "transactionBrazil");

        // Input file
        Path inputFile = new Path("in/operacoes_comerciais_inteira.csv");
        // Output file
        Path outputFile = new Path("output/question1.txt");


    }
}
