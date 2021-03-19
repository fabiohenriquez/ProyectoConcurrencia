
/*

Proyecto: Contador de Palabras con Hadoop - MapReduce
Clase: Concurrencia de Sistemas Distribuidos 

Integrantes:
Fabio Alejandro Henriquez    11711109
Leonard Ariel Banegas        11711179 

*/



import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ContadorPalabras {

 
 
  public static class Separacion extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text palabra_temp = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer palabra = new StringTokenizer(value.toString());

      
      while (palabra.hasMoreTokens()) {
        palabra_temp.set(palabra.nextToken());
        context.write(palabra_temp, one);
      }
    }
  }
 


  public static class ReduceClase extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable finale = new IntWritable();
    public void reduce(Text text, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sumaValores = 0;
      for (IntWritable val : values) {
        sumaValores += val.get();
      }
      finale.set(sumaValores);
      context.write(text, finale);
    }
  }
 
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Contador de Palabras Una");
    job.setJarByClass(ContadorPalabras.class);
    job.setMapperClass(Separacion.class);
    job.setCombinerClass(ReduceClase.class);
    job.setReducerClass(ReduceClase.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
