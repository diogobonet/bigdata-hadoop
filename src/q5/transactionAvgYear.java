package q5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class transactionAvgYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/pergunta5.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "job5");

        // registro das classes
        j.setJarByClass(transactionAvgYear.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce5.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(transactionAvgYearWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(LongWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, transactionAvgYearWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();

            if (linha.startsWith("country")){
                return;
            }

            String[] col = linha.split(";");

            if (col[0].equals("Brazil")){
                con.write(new Text(col[0]),new transactionAvgYearWritable(Long.parseLong(col[5]),new Long(1)));
            }
        }
    }

    public static class Reduce5 extends Reducer<Text, transactionAvgYearWritable, Text, LongWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<transactionAvgYearWritable> values, Context con)
                throws IOException, InterruptedException {

            long qtd = 0;
            long soma = 0;

            for (transactionAvgYearWritable valor:values){
                qtd += valor.getQtd();
                soma += valor.getValor();
            }

            long media = soma /qtd;

            con.write(key,new LongWritable(media));
        }
    }
}
