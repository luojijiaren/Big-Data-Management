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
import java.util.HashMap;
import java.io.*;
import java.net.URI;
import java.util.Vector;
//import java.io.FileReader;
//import java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

//public HashMap<String,String> ht = new HashMap<String, String>();
public class kmeans {

  public static enum Program_Counter{
    converge
  };



  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
 //   private final static one = new IntWritable(1);
    private Text word = new Text();
    private Text one = new Text();
    private Vector<String> centroids = new Vector<String>(10,10);

    //private File T1;
    @Override
    public void setup (Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		Path getPath = new Path(cacheFiles[0].getPath());
		BufferedReader br = null;
		String line = null;
	//	try{
			br = new BufferedReader(new InputStreamReader(fs.open(getPath)));
			int i =0;
			while((line = br.readLine())!=null)
        centroids.add(line); //add initials points to centroids
	//	}
	//	catch(Exception e){}
	}
  @Override
  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
	 String result="";
	 String val="";
    String point;	      
    //    while (itr.hasMoreTokens()) {
    point = itr.nextToken();
    String[] tokens =point.split(",");
    //if(tokens.length != 2)
     //return;
      //  }
    long max_dist = Long.MAX_VALUE;
    int max_index = 0;
    //assign points to centroids
    for(int i=0;i<centroids.size();i++)
    //for(int i=0;i<8;i++)
    {
      String[] coord = centroids.get(i).split(",");
      int x = Integer.parseInt(coord[0]);
      int y = Integer.parseInt(coord[1]);
      long dist = (long) Math.pow(x - Integer.parseInt(tokens[0]),2)+ (long) Math.pow(y - Integer.parseInt(tokens[1]),2);
      if(dist<max_dist)
      {
        result = centroids.get(i);
        max_dist = dist;
        max_index = i;
      }
    }
	 one.set(point+","+Integer.toString(max_index));//add which clusters it belongs to
	 //String ID = itr.nextToken().split(",")[0];
    word.set(result);
    context.write(word, one);
  
  }
}
  public static class IntSumCombiner 
       extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
  //  private Text key = new Text();
    private Vector<String> centroids = new Vector<String>();
   // @Override
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    long count = 0;
    long cum_x = 0;
    long cum_y = 0;
    String cluster = "";
    for(Text val : values)
    {   
       String[] tokens = val.toString().split(",");
       cum_x = Integer.parseInt(tokens[0]) + cum_x;
       cum_y = Integer.parseInt(tokens[1]) + cum_y;
       cluster = tokens[2];
       count++;
    }
    //count is never zero
    long new_x = cum_x/count;
    long new_y = cum_y/count;

    result.set(Long.toString(new_x)+","+Long.toString(new_y)+","+cluster);
    context.write(key,result);
    }
  }
 // @Override
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,NullWritable> {
    private Text result = new Text();
    private Vector<String> centroids = new Vector<String>();
//    @Override
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
    Path getPath = new Path(cacheFiles[0].getPath());
    BufferedReader br = null;
    String line = null;
    //try{
      br = new BufferedReader(new InputStreamReader(fs.open(getPath)));
    //  int j =0;
      while((line = br.readLine())!=null)
        centroids.add(line); //add initials points to centroids
  //  }
   // catch(Exception e){}

    long count = 0;
    long cum_x = 0;
    long cum_y = 0;
    String cluster="";

    for(Text val : values)
    {		
       String[] tokens = val.toString().split(",");
       cum_x = Integer.parseInt(tokens[0]) + cum_x;
       cum_y = Integer.parseInt(tokens[1]) + cum_y;
       cluster = tokens[2];
       count++;
    }
    //count is never zero
    long new_x = cum_x/count;
    long new_y = cum_y/count;
    //compare with last iterations
    int i = Integer.parseInt(cluster);
    String[] old_coord = centroids.get(i).split(",");
    int cent_x = Integer.parseInt(old_coord[0]);
    int cent_y = Integer.parseInt(old_coord[1]);
    if(cent_x == new_x && cent_y == new_y)
    {
      context.getCounter(Program_Counter.converge).increment(1);

      key.set(Long.toString(new_x)+","+Long.toString(new_y)+",this centroid is converged");
    }
    else key.set(Long.toString(new_x)+","+Long.toString(new_y)+",this centroid is not converged yet");

  //  key.set(key.toString()+"2");
    //context.write(key,NullWritable.get());
    context.write(key, NullWritable.get());
    }
  }


 
  public static void main(String[] args) throws Exception {
    //Configuration conf = new Configuration();
    if (args.length != 4) {
      System.err.println("Usage: <HDFS k points file> <HDFS all points file> <HDFS output file> <k value>");
      System.exit(2);
    }


    int counter = 0;
    int notchange = 0;
    String input=args[0];
    int k_value = Integer.parseInt(args[3]);
    long converge =0;
    //Configuration conf = new Configuration();

    while(counter<5 && converge != k_value)
    {
      Configuration conf = new Configuration();

      FileSystem fs = FileSystem.get(conf);
      Path cachefile = new Path(input);
      FileStatus[] list = fs.globStatus(cachefile);
      for(FileStatus status : list)
      DistributedCache.addCacheFile(status.getPath().toUri(),conf);
    
      Job job = new Job(conf, "kmeans clustering");

      job.setJarByClass(kmeans.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumCombiner.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setNumReduceTasks(1);
      job.setOutputValueClass(Text.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
  //  FileInputFormat.addInputPath(job, new Path(args[0]));
      FileInputFormat.addInputPath(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]+Integer.toString(counter)));
      job.waitForCompletion(true);
      //automatically exit the loop if all centroids converge
      converge = job.getCounters().findCounter(Program_Counter.converge).getValue();
      input = args[2]+Integer.toString(counter)+"/part-r-00000";
      counter++;
    }
      System.exit(0);
    }
}
