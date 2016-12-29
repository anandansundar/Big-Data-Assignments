package WordCount.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import WordCount.WordCount.Assignment2Q1.IntSumReducer;
import WordCount.WordCount.Assignment2Q1.TokenizerMapper;

public class Assignment2Q2 {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{

     private Text word = new Text();
     private Text word_two = new Text();
     private final static IntWritable one = new IntWritable(1);
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
   	  
   	  StringTokenizer st=new StringTokenizer(value.toString(), "^");
   	  int count=0;
   	  String address=null;
   	  String bussinessId=null;
   	  while(st.hasMoreTokens())
   	  {
   		  count++;
   		  if(count==1)
   		  {
   			bussinessId=st.nextToken().toString();
   		  }
   		  else if(count==2)
   		  {
   			  address=st.nextToken().toString();
   			  break;
   		  }
   		  else
   		  {
   			  st.nextToken();
   		  }
   	  }
   	  
   	  if(address!=null && address.contains("NY"))
   	  {
   		  word.set(address);
   		  word_two.set(bussinessId);
   		  context.write(word_two, new Text(address));
   	  }
     }
 
 }
	 
	 public static class IntSumReducer
    extends Reducer<Text,Text,Text,Text> {
		 
		 private Text word = new Text();

		 public void reduce(Text key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
		 for(Text words: values)
            context.write(key, words);     
		 }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Assignment 2 Q2");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(TokenizerMapper.class);
		    //job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
