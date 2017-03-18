package Q3_5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Q3_5 extends Configured implements Tool{

	public static class cusMap 
    extends Mapper<LongWritable, Text, Text, Text>{
	  private Text cId = new Text();
	  private Text cusName= new Text();
	  public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] nvalues = line.split(",");
		  String cusID = nvalues[0];
		  String name = nvalues[1];

		  cId.set(cusID);
		  cusName.set("cus"+","+name);
		  context.write(cId, cusName);
	  }
	}
	public static class transMap
		extends Mapper<LongWritable, Text, Text, Text>{
		private Text cId = new Text();

		private Text trans= new Text();

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String line = value.toString();
			String[] nvalues = line.split(",");
	  
			String cusID=nvalues[1];
			cId.set(cusID);
	  
			trans.set("trans");
			context.write(cId, trans);
	  
		}
	}

	public static class joinReducer 
		extends Reducer<Text, Text,NullWritable,Text> {
  
		String name;

		public void reduce(Text customersID, Iterable<Text> values , 
				Context context
				) throws IOException, InterruptedException {
			int sum=0;

			for (Text val : values) {
				String check=val.toString().split(",")[0];
				if (check.equals("cus")){
					name = val.toString().split(",")[1];
				}
				else if (check.equals("trans")){
					sum +=1;
				}
			}
	  
			Text results = new Text();
			results.set((name+","+Integer.toString(sum)));
			context.write(NullWritable.get(),results);
		}

	  
	}
  

	  
	public static class totalMap 
		extends Mapper<LongWritable, Text, Text, Text>{
	    String name;
	    String transTotal;
	    public void map(LongWritable key, Text value, Context context
               ) throws IOException, InterruptedException {
		  String line = value.toString();
		  String[] nvalues = line.split(",");
		  
		  name = nvalues[0];
		  transTotal= nvalues[1];
		  
		  Text results = new Text();
		  results.set(transTotal);
		  Text names = new Text();
		  names.set(name);
		  context.write(names, results);
	    }
	}
  
  public static class selectReducer 
  	extends Reducer<Text, Text,Text,Text> {
	          int sum=0;
	  		  int meanTrans ;
	  		  int sumTotal=0;
	  		  Text nameFinal = new Text();

	  protected void setup(Context context) throws java.io.IOException, InterruptedException{
		  meanTrans=100;
	  }

	  int transTotal;
	  public void reduce(Text name, Iterable<Text> values , 
                  Context context
                  ) throws IOException, InterruptedException {
		    
		  for (Text val : values) {
			  sum++;
			  transTotal = Integer.parseInt(val.toString());
			  sumTotal+=transTotal;}
		       
		      meanTrans=sumTotal/sum;
			  if (transTotal>meanTrans){
				   
				   nameFinal=name;
				   context.write(nameFinal,new Text(""));

			  }
		  
		  
	}
	
		  
  }
  


  private static final String OUTPUT_PATH = "ioutput53";
	
	@Override
	 public int run(String[] args) throws Exception {
	
	  Configuration conf = getConf();
	  Job job = new Job(conf, "Job1");
	  job.setJarByClass(Q3_5.class);
	  Path cusInputPath = new Path(args[0]); 
	  Path transInputPath = new Path(args[1]); 
	  Path outputPath = new Path(args[2]);
	  
	  MultipleInputs.addInputPath(job, cusInputPath,
	            TextInputFormat.class, cusMap.class);
	  MultipleInputs.addInputPath(job, transInputPath,
	            TextInputFormat.class, transMap.class);
	    
	  for (String arg : args) {
		  System.out.println(arg);
	  }
	  job.setReducerClass(joinReducer.class);

	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);

	  
	  job.setOutputFormatClass(TextOutputFormat.class);
	  
	  FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	  job.waitForCompletion(true);

	  
	  Job job2 = new Job(conf, "Job2");
	  job2.setJarByClass(Q3_5.class);

	  job2.setMapperClass(totalMap.class);
	  job2.setReducerClass(selectReducer.class);

	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);

	  job2.setInputFormatClass(TextInputFormat.class);
	  job2.setOutputFormatClass(TextOutputFormat.class);

	  FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	  FileOutputFormat.setOutputPath(job2, outputPath);

	  return job2.waitForCompletion(true) ? 0 : 1;
	 }

	
	public static void main(String[] args) throws Exception{
	// TODO Auto-generated method stub
		if (args.length != 3) {
		      System.err.println("error");
		      System.exit(3);
		    }	
  	ToolRunner.run(new Configuration(),new Q3_5(), args);
	}
}


