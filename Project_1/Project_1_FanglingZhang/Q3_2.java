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
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text cid = new Text();
		private Text trSum = new Text();

		public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {

          StringTokenizer tr = new StringTokenizer(value.toString());

          while (tr.hasMoreTokens()) {
      	  String[] tokens =tr.nextToken().split(",");
     	  if(tokens.length != 5)
		  return;
	
	      String t1 = tokens[1];
	      String t2 = tokens[2];
          cid.set(t1);
	      trSum.set(t2);
          output.collect(cid, trSum);
		  }
	   }
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, 
				Reporter reporter)throws IOException {

     		long count = 0;
			float sum = 0.00000f;
			while (values.hasNext()) {
				String[] s1 = values.next().toString().split(" ");
				float s2 = Float.parseFloat(s1[0]);
				sum += s2;
				count += 1;
			}
			
			String s3 = Float.toString(sum);
			String c = Long.toString(count);
			Text result = new Text();
			result.set(s3 + "    " + c);
			output.collect(key, result);
			}
	}



	public static void main(String[] args) throws Exception{
	     JobConf conf = new JobConf(Q3_2.class);
	     conf.setJobName("Q3_2");
	     
	     conf.setMapOutputKeyClass(Text.class);
	     conf.setMapOutputValueClass(Text.class);
	     
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.addInputPath(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	}

}