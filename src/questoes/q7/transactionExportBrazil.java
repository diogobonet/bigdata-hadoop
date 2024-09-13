package questoes.q7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class transactionExportBrazil {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // Utilizado para configurar o Log4j (usado pelo hadoop)
        Configuration hadoopConfig = new Configuration();

        // Caminhos para arquivo de entrada e saída.
        Path inputFilePath  = new Path("in/operacoes_comerciais_inteira.csv");
        Path outputFilePath = new Path("output/q7_resultado");

        // Criação do objeto Job com base na configuração criada anteriormente.
        Job hadoopMapReduceJob = Job.getInstance(hadoopConfig, "transactionExportBrazil");

        // Onde são definidas as classes usadas para esse Job
        hadoopMapReduceJob.setJarByClass(transactionExportBrazil.class);
        hadoopMapReduceJob.setMapperClass(transactionExportBrazilMapper.class);
        hadoopMapReduceJob.setReducerClass(transactionExportBrazilReducer.class);


        // Onde são definidos os tipos de saída do Map
        hadoopMapReduceJob.setMapOutputKeyClass(Text.class);
        hadoopMapReduceJob.setMapOutputValueClass(averageWritable.class);
        hadoopMapReduceJob.setOutputKeyClass(Text.class);
        hadoopMapReduceJob.setOutputValueClass(LongWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(hadoopMapReduceJob, inputFilePath);
        FileOutputFormat.setOutputPath(hadoopMapReduceJob, outputFilePath);

        // Inicia o Job configurado anteriormente e aguarda o final da execução.
        //      Caso tenha sido executado com sucesso, será retornado "true"
        //      Caso tenha tido um problema ao executar, será retornado "false"
        Boolean isCompletedSuccessfully = hadoopMapReduceJob.waitForCompletion(true);
        int successCode = 0;
        int errorCode   = 1;
        if (isCompletedSuccessfully) System.exit(successCode); // Encerra aplicativo com código sem erros
        System.exit(errorCode); // Encerra aplicativo com código de erro
    }
}