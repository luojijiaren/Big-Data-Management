package org;



import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class Problem2 {
	
	public static class jsonInputFormat extends FileInputFormat<LongWritable,Text>{
		
	    @Override
	    public RecordReader<LongWritable, Text> createRecordReader(
	            InputSplit split, TaskAttemptContext context) throws IOException,
	            InterruptedException {
	        return new jsonLineRecordReader();
	    }
	    
	    protected boolean isSplitable(FileSystem fs, Path filename) {
	    	return true;	//input file is splitable
	    }
	    
//	    public static void setMaxInputSplitSize(Job job, long size) {
//	    	
//	    }
	    protected long computeSplitSize(long blockSiz, long minSize, long maxSize) {
	    	return 204800;
	    }
	    
	}
	
	
	public static class jsonLineRecordReader extends RecordReader<LongWritable, Text> {
		 
	    private long start;
	    private long pos;
	    private long end;
	    private LineReader in;
	    private int maxLineLength;
	    private Text newValue = new Text();
	 	 
	    public void initialize(
	            InputSplit genericSplit, 
	            TaskAttemptContext context)
	            throws IOException {
	 
	        FileSplit split = (FileSplit) genericSplit;
	 
	        Configuration job = context.getConfiguration();
	        this.maxLineLength = job.getInt(
	                "mapred.linerecordreader.maxlength",
	                Integer.MAX_VALUE);
	 
	        start = split.getStart();
	        end = start + split.getLength();
	 
	        final Path file = split.getPath();
	        FileSystem fs = file.getFileSystem(job);
	        FSDataInputStream fileIn = fs.open(split.getPath());
	 
	        boolean skipFirstLine = false;
	        if (start != 0) {
	            skipFirstLine = true;
	            --start;
	            fileIn.seek(start);
	        }
	 
	        in = new LineReader(fileIn, job);
	 
	        if (skipFirstLine) {
	            start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
	        }
	 
	        this.pos = start;
	 
	    }
	 
	    @Override
	    public boolean nextKeyValue() throws IOException {
	 
	    	LongWritable key = new LongWritable();
	    	Text value = new Text();
	    	
	    	String line = "-1" ;
	    	int newSize=0;
			value.clear();
	        while (pos < end) {
	        	key.set(pos);
	            newSize = in.readLine(value, maxLineLength,
	                    Math.max((int) Math.min(
	                            Integer.MAX_VALUE, end - pos),
	                            maxLineLength));
	 
	            if (newSize == 0) {
	                return false;
	            }
	            pos += newSize;
	            line = value.toString();
	            
	            while(!line.contains("\"")){
	            
	            	newSize = in.readLine(value, maxLineLength,
	                        Math.max((int) Math.min(
	                                Integer.MAX_VALUE, end - pos),
	                                maxLineLength));
	            	if (newSize == 0) {
	                    return false;
	                }
	                pos += newSize;
	                line = value.toString();
	            }
	            
	            newValue.set(line.split(":")[1].replace("\n",","));
	            newSize = in.readLine(value, maxLineLength,
	                    Math.max((int) Math.min(
	                            Integer.MAX_VALUE, end - pos),
	                            maxLineLength));
				pos += newSize;
	            line = value.toString();
	            while(!line.contains("}"))
				   {
	            	   newValue.set(newValue.toString()+line.split(":")[1].replace("\n",","));
	            	   newSize = in.readLine(value, maxLineLength,
	                           Math.max((int) Math.min(
	                                   Integer.MAX_VALUE, end - pos),
	                                   maxLineLength));
	            	   if (newSize == 0) 
					   {
							return false;
					   }
					   pos += newSize;
			           line = value.toString();
				   }
	            
	            if (newSize < maxLineLength) {
	                return true;
	            }   
	        }
	        return false;
	    }
	 
	    @Override
	    public LongWritable getCurrentKey() throws IOException,
	            InterruptedException {
	    	LongWritable posKey = new LongWritable();
	    	posKey.set(pos);
	    	
	        return posKey;
	    }
	 
	    @Override
	    public Text getCurrentValue() throws IOException, InterruptedException {
	        return newValue;
	    }
	 
	    @Override
	    public float getProgress() throws IOException, InterruptedException {
	        if (start == end) {
	            return 0.0f;
	        } else {
	            return Math.min(1.0f, (pos - start) / (float) (end - start));
	        }
	    }
	 
	    @Override
	    public void close() throws IOException {
	        if (in != null) {
	            in.close();
	        }
	    }

		
		
	 
	}

	
	public static class Map extends
		Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
						
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{  
	        String line = value.toString();
	        String[] str = line.split(",");
	        if (str.length==13){
				String flag = str[5];
				outKey.set(flag);
				outValue.set(Integer.toString(1));
		        context.write(outKey, outValue);
	        }
	        
	        
        } 
	}

	public static class Reduce 
  		extends Reducer<Text, Text,Text,Text> {

	    public void reduce(Text key, Iterable<Text> values , 
                  Context context) throws IOException, InterruptedException {
	    	
			Integer sum = 0;
			Text results = new Text();
		
	    	for (Text val:values){
				 sum = sum + 1;
	    	}
	    	
	    	results.set( sum.toString());
	    	context.write(key,results);
	    }		  
	}	
	

	public static void main(String[] args) throws Exception  
    {  
        
		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJobName("problem2");  
		job.setJarByClass(Problem2.class);

        job.setMapperClass(Map.class);  
        job.setReducerClass(Reduce.class);  
        
  	  	
        
  	    FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileInputFormat.setMaxInputSplitSize(job, 204800);
		FileInputFormat.setMinInputSplitSize(job, 1);
		
		job.setInputFormatClass(jsonInputFormat.class);

        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);  
          
  	  	job.setOutputKeyClass(Text.class);
  	  	job.setOutputValueClass(Text.class); 
  
  	    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
	}

	
}