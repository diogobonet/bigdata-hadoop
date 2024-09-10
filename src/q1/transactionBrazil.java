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

// Número de transações envolvendo o Brasil.
public class transactionBrazil {
    public static void main(String[] args) throws Exception  {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        // Job creation
        Job job = Job.getInstance(c, "transactionBrazil");

        // Input file
        Path inputFile = new Path("in/operacoes_comerciais_inteira.csv");
        // Output file
        Path outputFile = new Path("output/question1.txt");

        // Set classes
        job.setJarByClass(transactionBrazil.class);
        job.setJarByClass(Map.class);
        job.setJarByClass(Reduce.class);

        // definicao dos tipos de saida
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and Output files registration
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        // Job and wait for execution
        boolean success = job.waitForCompletion(true);
        if (!success) {
            System.err.println("Job failed.");
        }
    }

    public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String linha = value.toString();

            if (linha.startsWith("country")) {
                return;
            }

            String[] col = linha.split(";");

            if (col[0].equals("Brazil")) {
                Text chavesaida = new Text(col[0]);
                con.write(chavesaida, new IntWritable(1));
            }
        }
    }

    public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values){
                sum += value.get();
            }

            con.write(key,new IntWritable(sum));
        }
    }
}

