package Q3_3;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Q3_3 extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		 
		private HashMap<String,String> cust = new HashMap<String,String>();
		private IntWritable outKey = new IntWritable();   
        private Text outValue = new Text(); 
		
		protected void setup(Context context) throws IOException{
			Path[] cacheP = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cacheP){
				if(p.toString().endsWith("customers")){
					BufferedReader br = new BufferedReader(new FileReader(p.toString())); 
					while(br.readLine() != null){
						String[] str = br.readLine().split(",",5);
						cust.put(str[0], str[1]+","+str[4]);
					}
				}
			}	
		}


	
		public void map(LongWritable key, Text value,Context c2) throws IOException, InterruptedException{
			
			String v = value.toString();
			String[] vs = v.split(",", 5);
			String info = cust.get(vs[1]);

			if(info != null){
				outKey.set(Integer.parseInt(vs[1]));
				outValue.set(info + ","+ vs[2]+","+vs[3]);
				c2.write(outKey, outValue);
			}

		}
	
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		private Text result = new Text();
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int min = Integer.MAX_VALUE;
		    int num = 0;
		    double sum = 0;
		    String name = null;
		    String salary = null;



			
			Iterator<Text> value = values.iterator();
			while(value.hasNext()){
				String va = value.next().toString();
				String[] vas = va.split(",");
				name = vas[0];
				salary = vas[1];
				num ++;
				sum += Double.parseDouble(vas[2]);
				if(Integer.parseInt(vas[3]) < min){
					min = Integer.parseInt(vas[3]);
				}
			}

			String transSum = Double.toString(sum);
			String transNum = Long.toString(num);
			String Item_min = Long.toString(min);			
			String outTr = name + "  " + salary + "  " + transNum+ "  " +transSum +"  "+Item_min;
			
			Text out = new Text();
			out.set(outTr);			
			context.write(key, out);


		}
	}
	
	 public int run(String[] args) throws Exception {

		Job job = new Job();
		Configuration conf = job.getConfiguration();   
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);  
	   
	    job.setJobName("Q3_3");
	    job.setJarByClass(Q3_3.class);
			
		     
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
		
    	job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
	

	    }
		public static void main(String[] args) throws Exception {
		int returnCode =  ToolRunner.run(new Q3_3(),args); 
		}

}
