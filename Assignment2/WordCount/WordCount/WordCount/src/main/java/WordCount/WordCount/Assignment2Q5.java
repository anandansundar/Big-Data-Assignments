package WordCount.WordCount;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import WordCount.WordCount.Assignment2Q4.IntSumReducer;
import WordCount.WordCount.Assignment2Q4.TokenizerMapper;

public class Assignment2Q5 {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, DoubleWritable>{

     private Text word = new Text();
     private final static DoubleWritable one = new DoubleWritable(1);
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
   	  
   	  StringTokenizer st=new StringTokenizer(value.toString(), "^");
   	  int count=0;
   	  String business=null;
   	  String star=null;
   	  while(st.hasMoreTokens())
   	  {
   		  count++;
   		  
   		  if(count==3)
   		  {
   			business=st.nextToken().toString();
   		  }
   		  else if(count==4)
   		  {
   			star=st.nextToken().toString();
   		  }
   		  else
   		  {
   			  st.nextToken();
   		  }
   	  }
   	  
   	 word.set(business);
   	 
     double result=Double.parseDouble(star);
     context.write(word, new DoubleWritable(result));
   	
     }
 
 }
	 
	 public static class IntSumReducer
    extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		 private DoubleWritable result = new DoubleWritable();
		 private Text word = new Text();
         Map<String,Double> map=new HashMap<String,Double>();
         
		 public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context
                ) throws IOException, InterruptedException {
       int sum = 0;
       int count=0;
       for (DoubleWritable val : values) {
           sum += val.get();
           count=count+1;
         }
         map.put(key.toString(), (double)sum/(double)count);  
		 }
		 
		 @Override
		protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<String,Double> map1=sortByValue(map);
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
		 
		 private static Map<String, Double> sortByValue(Map<String, Double> unsortMap) {

		        List<Map.Entry<String, Double>> list =
		                new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());

		        Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
		            public int compare(Map.Entry<String, Double> o1,
		                               Map.Entry<String, Double> o2) {
		                return (o1.getValue()).compareTo(o2.getValue());
		            }
		        });

		        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		        for (Map.Entry<String, Double> entry : list) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		        }

		        return sortedMap;
		    }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Assignment 2 Q5");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(TokenizerMapper.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
