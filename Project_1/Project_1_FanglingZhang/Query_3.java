/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.examples;

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
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Query_3 extends Configured implements Tool{

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
 //   private final static one = new IntWritable(1);
    private Text word = new Text();
    private Text one = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	String result;
	String val;	      
      while (itr.hasMoreTokens()) {
      	String[] tokens =itr.nextToken().split(",");
     	 if(tokens.length != 5)
		return;
	
	if(tokens[1].length() >=10) // Customers
	{
	  result=tokens[0];
	  val = tokens[1]+","+tokens[4];
	}
	else{ //Transactions
	  result = tokens[1];
	  val = "1,"+tokens[2]+","+tokens[3];
	}
	//result = result+","+"1";
	one.set(val);
	//String ID = itr.nextToken().split(",")[0];
        word.set(result);
        context.write(word, one);
  }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      String name ="";
      String Salary="";
      float transTotal = 0;
      int numTrans = 0;
      int minItems = 10;//max is 10
    //  for (IntWritable val : values) {
      //  sum += val.get();
      //}

      for(Text val : values)
      {		
	String[] tokens = val.toString().split(",");
	if(tokens.length ==3)//transactions
	{
  	    transTotal = transTotal+ Float.parseFloat(tokens[1]);
	    numTrans = numTrans+ Integer.parseInt(tokens[0]);
	    minItems = Math.min(minItems, Integer.parseInt(tokens[2]));
	}
	else if (tokens.length==2){//customers
	    if(name.equals(""))
		name = tokens[0];
	    if(Salary.equals(""))
		Salary = tokens[1];
	}
	else{//transactions+customers
	    if(name.equals(""))
		name = tokens[0];
	    if(Salary.equals(""))
		Salary = tokens[1];
            transTotal = transTotal+ Float.parseFloat(tokens[3]);
	    numTrans = numTrans+ Integer.parseInt(tokens[2]);
	    minItems = Math.min(minItems, Integer.parseInt(tokens[4]));
	}

      }
      String res = name+","+Salary+","+Integer.toString(numTrans)+","+Float.toString(transTotal)+","+Integer.toString(minItems);
      result.set(res);
      context.write(key, result);
    }
  }

    public int run(String[] args) throws Exception {
  //public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 3) {
      System.err.println("Usage: Query_3 <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "Query_3");
    job.setJarByClass(Query_3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(3);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
   // MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class);
    //MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class);
    	    job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;

  }
  	public static void main(String[] args) throws Exception {
		int returnCode =  ToolRunner.run(new Query_3(),args); 
		}
}
