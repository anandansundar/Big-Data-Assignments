package WordCount.WordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import WordCount.WordCount.Assignment2Q1.IntSumReducer;
import WordCount.WordCount.Assignment2Q1.TokenizerMapper;

public class Assignment2Q3 {


	 public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

     private Text word = new Text();
     private final static IntWritable one = new IntWritable(1);
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
   	  
   	  StringTokenizer st=new StringTokenizer(value.toString(), "^");
   	  int count=0;
   	  String address=null;
   	  while(st.hasMoreTokens())
   	  {
   		  count++;
   		  
   		  if(count==2)
   		  {
   			  address=st.nextToken().toString();
   			  break;
   		  }
   		  else
   		  {
   			  st.nextToken();
   		  }
   	  }
   	  
   	 String addresss[]=address.split(" ");
   	 word.set(addresss[addresss.length-1]);
   	 context.write(word, one);
     }
 
 }
	 
	 public static class IntSumCombainer
	    extends Reducer<Text,IntWritable,Text,IntWritable> {
			 private IntWritable result = new IntWritable();
			 private Text word = new Text();

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
	 
	 public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		 private Text word = new Text();
         Map<String,Integer> map=new HashMap<String,Integer>();
         
		 public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
       int sum = 0;
       
       for (IntWritable val : values) {
           sum += val.get();
         }
         map.put(key.toString(), sum);  
		 }
		 
		 @Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<String,Integer> map1=sortByValue(map);
			int count=0;
			for(String s: map1.keySet())
			{
				result.set(map1.get(s));
				word.set(s);
				context.write(word, result);
				count++;
				if(count==10)
					break;
			}
			
		}
		 
		 private static Map<String, Integer> sortByValue(Map<String, Integer> unsortMap) {

		        List<Map.Entry<String, Integer>> list =
		                new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());

		        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
		            public int compare(Map.Entry<String, Integer> o1,
		                               Map.Entry<String, Integer> o2) {
		                return (o2.getValue()).compareTo(o1.getValue());
		            }
		        });

		        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		        for (Map.Entry<String, Integer> entry : list) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		        }

		        return sortedMap;
		    }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Assignment 2 Q3");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(TokenizerMapper.class);
		    job.setCombinerClass(IntSumCombainer.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
