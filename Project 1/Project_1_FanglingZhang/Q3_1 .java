package Q3_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3_1 {

  public static class TokenizerMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    private Text custID = new Text();    
    private Text outString = new Text();

      
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter
                    ) throws IOException {

      String line = value.toString();
      String[] record = line.split(",");
      Integer code= Integer.parseInt(record[3]);
      

      if (code>=2 && code<=6) {   
        custID.set(record[0]);
        outString.set(record[1]+","+record[2]+","+record[3]+","+record[4]);
        output.collect(custID, outString);
      }
    }
  }
  

  public static void main(String[] args) throws Exception {
	JobConf job = new JobConf(Q3_1.class);
 	job.setJobName("Q3_1");
    job.setMapperClass(TokenizerMapper.class);

    job.setOutputKeyClass(Text.class);

    job.setOutputValueClass(Text.class);

    job.setInputFormat(TextInputFormat.class);
    job.setoutputFormat(TextoutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    JobClient.runJob(job);
  }
}
