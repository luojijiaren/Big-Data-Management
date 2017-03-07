package Q3_1;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

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
	JobConf conf = new JobConf(Q3_1.class);
 	conf.setJobName("Q3_1");
    conf.setMapperClass(TokenizerMapper.class);

    conf.setOutputKeyClass(Text.class);

    conf.setOutputValueClass(Text.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
