package Q3_2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Q3_2 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

		public void map(LongWritable key, Text value,OutputCollector<IntWritable, DoubleWritable> output, 
				Reporter reporter) throws IOException {
			String line = value.toString();
			String[] record = line.split(",");
			int custId = Integer.parseInt(record[1]);
			double transTotal = Double.parseDouble(record[2]);

			private IntWritable cid = new IntWritable(cusId);
			private DoubleWritable transum = new DoubleWritable(transTotal);
			output.collect(cid, transum);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
		private Text tranInfo = new Text();
		
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,OutputCollector<IntWritable, Text> output, 
				Reporter reporter)throws IOException {

				int transNum = 0;
				double transSum = 0;

				while (values.hasNext()) {
					transNum += 1;
					transSum += values.next().get();
				}
			    String num = Double.toString(transNum);
			    String sum = Long.toString(transSum);
			    Text trans = new Text();
			    trans.set(num + "," + sum);
				output.collect(key, trans);
			}
	}



	public static void main(String[] args) throws Exception{
	     JobConf conf = new JobConf(Q3_2.class);
	     conf.setJobName("Q3_2");
	     
	     conf.setMapOutputKeyClass(IntWritable.class);
	     conf.setMapOutputValueClass(DoubleWritable.class);
	     
	     conf.setOutputKeyClass(IntWritable.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	}

}