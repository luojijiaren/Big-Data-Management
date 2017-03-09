package Q3_3;


import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.lang.Math;
import java.lang.Object;




public class Q3_3 extends Configured implements Tool {
	public static class Map{
		 
		private HashMap<String,String> cust = new HashMap<String,String>();
        private IntWritable outPutKey = new IntWritable();   
        private Text outPutValue = new Text(); 
		
		protected void setup(Context context) throws IOException{
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cachePaths){
				if(p.toString().endsWith("customers")){
					BufferedReader br = new BufferedReader(new FileReader(p.toString())); 
					while(br.readLine() != null){
						String[] str = br.readLine().split(",",5);
						cust.put(str[0], str[1]+","+str[4]);
					}
				}
			}	
		}


	
		public void map(LongWritable key, Text value,OutputCollector<IntWritable, Text> output, 
				Reporter reporter) throws IOException, InterruptedException{
			
			String line = value.toString();
			String []lineSplit = line.split(",", 5);
			String info = cust.get(lineSplit[1]);
			if(info != null){
				outPutKey.set(Integer.parseInt(lineSplit[1]));
				outPutValue.set(info + ","+ lineSplit[2]+","+lineSplit[3]);
				output.collect(outPutKey, outPutValue);
			}

		}
	
	}
	
	public static class Reduce{
		int minItem = Integer.MAX_VALUE;
		int num = 0;
		double sum = 0;
		String name = null;
		String salary = null;
        
        private Text outPutValue1 = new Text(); 
		
		public void reduce(IntWritable key, Iterable<Text> values, OutputCollector<IntWritable, Text> output, 
				Reporter reporter)throws IOException, InterruptedException{
			
			Iterator<Text> value = values.iterator();
			while(value.hasNext()){
				String line = value.next().toString();
				String[] joinInfo = line.split(",");
				name = joinInfo[0];
				salary = joinInfo[1];
				num ++;
				sum += Double.parseDouble(joinInfo[2]);
				if(Integer.parseInt(joinInfo[3]) < minItem){
					minItem = Integer.parseInt(joinInfo[3]);
				}
			}

			String transSum = Double.toString(sum);
			String transNum = Long.toString(num);
			String Item_min = Long.toString(minItem);			
			String outTr = name + " " + salary + " " + transNum+ " " +transSum +" "+Item_min;
			
			Text out = new Text();
			out.set(outTr);			
			output.collect(key, out);

		}
	}
	
	 public int run(String[] args) throws Exception {
		
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Q3_3");
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);  
	    job.setJarByClass(Q3_3.class);
			
		     
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
		
    	job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
	

	    }
		public static void main(String[] args) throws Exception {
		int returnCode =  ToolRunner.run(new Q3_3(),args); 
		}

}
