package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
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

public class WordCountHash {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    HashSet<String> stopwords=new HashSet<String>();
    
    @Override
    protected void setup(Mapper.Context context){

        Configuration conf = context.getConfiguration();
        String stp_file_name = "https://github.com/naveenrajceg13/Naive-Bayes-and-Logistic-Regression-/blob/master/NXP154130_ML_Assignent2_Outer_Folder/NXP154130_ML_Assignment2/STOP_WORDS.txt";
        if(stp_file_name == null)
            return;
        
        BufferedReader fis;
        try {
        	InputStream in = new URL(stp_file_name).openStream();
            fis = new BufferedReader(new InputStreamReader(in));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Could not open stopwords file ",e);
        }
        String word;
        try {
            while((word =fis.readLine()) != null){
                stopwords.add(word);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("error while reading stopwords",e);
        }
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  String word_now=itr.nextToken();
    	  if(stopwords.contains(word_now))
    		  continue;
    	  if(word_now.charAt(0)!='#')
    		  continue;
        word.set(word_now);
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for(int i=0;i<args.length-1;i++)
    {
    FileInputFormat.addInputPath(job, new Path(args[i]));
    }
    FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}