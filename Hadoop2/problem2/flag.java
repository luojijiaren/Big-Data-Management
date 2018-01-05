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
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class flag {

  public static class jsonLineRecordReader extends RecordReader<LongWritable, Text>{
  	private long m_start;
  	private long m_end;
  	private long m_cur;
  	private LineReader in;
  	private int m_maxlength;
  	private Text val = new Text();


  	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException{
  		FileSplit split = (FileSplit) genericSplit;
  		Configuration conf = context.getConfiguration();
  		m_maxlength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
  		m_start = split.getStart();
  		m_end = m_start + split.getLength();

  		final Path file = split.getPath();
  		FileSystem fs = file.getFileSystem(conf);
  		FSDataInputStream fileIn = fs.open(split.getPath());

  		boolean skipFirstLine = false;
  		if(m_start != 0){
  			skipFirstLine = true;
  			m_start--;
  			fileIn.seek(m_start);
  		}
  		in = new LineReader(fileIn, conf);
  		if(skipFirstLine){
  			m_start += in.readLine(new Text(),0,(int) Math.min((long) Integer.MAX_VALUE, m_end - m_start));
  		}

  		m_cur = m_start;
  	}


    @Override 
    public boolean nextKeyValue() throws IOException{
    	LongWritable key = new LongWritable();
    	Text value = new Text();
      val.set("");
    	String line = "";
    	int readSize = 0;
    	while(m_cur < m_end){
    		key.set(m_cur);

        //skip everything til the start of next record
    		while(!line.contains("\"")){
    			readSize = in.readLine(value,m_maxlength, (int) Math.max(m_end-m_cur, m_maxlength));
    			if(readSize ==0)
    				return false;
    			m_cur +=readSize;
    			line = value.toString();
    		}

    		//parsing records til next } 
    		while(!line.contains("}"))
    		{
    			val.set(val.toString()+line.split(":")[1].replace("\n",","));
    			readSize = in.readLine(value, m_maxlength, (int) Math.max(m_end-m_cur, m_maxlength));
    			if(readSize ==0)
    				return false;
    			m_cur+= readSize;
    			line = value.toString();

    		}

    		if(readSize < m_maxlength)
    			return true;
    	}
    	 return false;

    }


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException{
    	LongWritable key = new LongWritable();
    	key.set(m_cur);
    	return key;
    }


    @Override 
    public Text getCurrentValue() throws IOException, InterruptedException{
    	return val;
    }
    

    @Override 
    public float getProgress() throws IOException, InterruptedException{
    	if(m_start == m_end)
    	  return 0.0f;
    	else return Math.min(1.0f,(m_cur-m_start) / (float)(m_end-m_start));
    }


    @Override 
    public void close() throws IOException{
    	if(in!=null)
    	  in.close();
    }
}



  public static class jsonInputFormat extends FileInputFormat<LongWritable, Text>{
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException{
        return new jsonLineRecordReader();
      }
    protected boolean isSplitable(FileSystem fs, Path filename)
    {
      return true;
    }
    //hardcoded the size of each splits so that we can have 5 mappers
    protected long computeSplitSize(long blockSize, long minSize, long maxSize){
      return 220000;
    }
}



  public static class TokenizerMapper 
       extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String line = value.toString();
      String[] tokens = line.split(",");
      if(tokens.length!=13)
          return;
	    String flag = tokens[5];

      word.set(flag);
      context.write(word, one);
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
    if (args.length != 2) {
      System.err.println("Usage: flag <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "jsonreader");
    job.setJarByClass(flag.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);


    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(1);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setInputFormatClass(jsonInputFormat.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}