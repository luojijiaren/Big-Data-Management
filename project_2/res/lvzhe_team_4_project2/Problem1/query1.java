package problem1;

import java.util.List;
import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class query1 extends Configured implements Tool {
	public static class PointsMap extends Mapper<LongWritable, Text, Text, Text> {
		public static String window;
		public static String pKey="";
		public static String pValue="";

		public void setup(Context context) throws IOException,InterruptedException {
			 window = context.getConfiguration().get("window");

		}

		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
					String[] points= value.toString().split(",");
					float x= Float.parseFloat(points[0]);
					float y= Float.parseFloat(points[1]);
					//int scale=10;


					String[] windows= window.split(",");
					int x1 = Integer.parseInt(windows[0]);
            		int y1 = Integer.parseInt(windows[1]);
            		int x2 = Integer.parseInt(windows[2]);
            		int y2 = Integer.parseInt(windows[3]);
            		int temp=0;

            		if(x2<x1){
            			temp=x1;
            			x1=x2;
            			x2=temp;
            		}

            		if(y2<y1){
            			temp=y1;
            			y1=y2;
            			y2=temp;
            		}


            		 if (x >= x1 && x <=x2 && y>=y1 && y<=y2){
            			int keyx = Math.round(x);
            			int keyy = Math.round(y);

            			pKey=String.valueOf(keyx+","+keyy);
            			pValue=x + "," + y;
            			Text outKey=new Text();
            			Text outValue=new Text();
            			outKey.set(pKey);
            			outValue.set(pValue);
            			context.write(outKey, outValue);

            		}

				}

		}

	public static class RectangleMap extends Mapper<LongWritable, Text, Text, Text> {
		public static String window;
		public static String pKey="";
		public static String pValue="";
		public void setup(Context context) throws IOException,InterruptedException {
			 window = context.getConfiguration().get("window");

		}

		public void map(LongWritable key, Text value, Context context) 
					throws IOException, InterruptedException{  
	            	String[] rect = value.toString().split(",");
	            	String rId = rect[0];
	            	float x = Float.parseFloat(rect[1]);  
	            	float y = Float.parseFloat(rect[2]);
	           		float width = Float.parseFloat(rect[3]);
	                float height = Float.parseFloat(rect[4]);
	            	//int scale=10000;

	            	String[]windows= window.split(",");
					int x1 = Integer.parseInt(windows[0]);
            		int y1 = Integer.parseInt(windows[1]);
            		int x2 = Integer.parseInt(windows[2]);
            		int y2 = Integer.parseInt(windows[3]);
            		int temp=0;

            		if(x2<x1){
            			temp=x1;
            			x1=x2;
            			x2=temp;
            		}

            		if(y2<y1){
            			temp=y1;
            			y1=y2;
            			y2=temp;
            		}

            		 if (!((x+width)<x1 || x>x2 || y<y1 ||(y-height)>y2)){

            		 	
            		 	int i;
            		 	int j;
            		 	for(i=Math.round(x);i<(x+width);i++){
            		 		for(j=Math.round(y-height);j<y;j++){
            		 			pKey=String.valueOf(i+","+j);          		 			
            					pValue=rId+","+ x + "," + y + "," + width +","+height;
            					Text outKey=new Text();
            					Text outValue=new Text();
            					outKey.set(pKey);
            					outValue.set(pValue);
            					context.write(outKey, outValue);

            		 		}

            		 	}

            		 }
            }
        }

    public static class Reduce 
  		extends Reducer<Text, Text,NullWritable,Text> {
	  	List<String> points = new ArrayList<>();
	  	List<String> rect = new ArrayList<>();
	  	
	  	Text results = new Text();
		NullWritable nw = NullWritable.get();
		//Text results1 = new Text();

	    public void reduce(Text key, Iterable<Text> values , 
                  Context context) throws IOException, InterruptedException {


	    	for (Text val : values) {
	    		//String nnvalue=val.toString();
	    		//results1.set(nnvalue);
			  String[] nvalue = val.toString().split(",");
			  if(nvalue.length==2){
				  points.add(val.toString());
			  }
			  else{
				  rect.add(val.toString());
			  }
			
		    }
	    	
	    	
		    for(String r : rect){

		    	String[] nrect = r.split(",");
	            String rId = nrect[0];
	            float rx = Float.parseFloat(nrect[1]);  
	            float ry = Float.parseFloat(nrect[2]);
	           	float width = Float.parseFloat(nrect[3]);
	            float height = Float.parseFloat(nrect[4]);

	            for(String p:points){
				  String np[] = p.split(",");
				    float x = Float.parseFloat(np[0]);  
	            	float y = Float.parseFloat(np[1]);
	            	if(x>=rx&&x<=(rx+width)&&y<=ry&&y>=(ry-height)){
	            		 results.set("<" + rId + ",(" + x + "," + y + ")>");
	            		 context.write(nw, results);    
	            	}
		    }

         }
        points.clear();
        rect.clear();
        
	    }
	} 


 	public int run(String[] args) throws Exception {

 		Configuration conf = new Configuration();
 		int X1 = 0;
		int Y1=10001;
		int X2=10001;
		int Y2=0;
		String omit = "";
		Scanner dd = new Scanner(System.in);
		
		System.out.println("Do you need to define a window? (y/n) : ");
		omit = dd.next();
		
		if (omit.contains("n")) {
			conf.set("window", "0,10001,10001,0");
		}
			
		else {
			do {
				System.out.println("Enter X1: ");
				X1 = dd.nextInt();
			} while (X1 < 1 || X1 > 10000);
			do {
				System.out.println("Enter Y1: ");
				Y1 = dd.nextInt();
			} while (Y1 < 1 || Y1 > 10000);
			do {
				System.out.println("Enter X2: ");
				X2 = dd.nextInt();
			} while (X2 < 1 || X2 > 10000 || (X1 == X2));
	
			do {
				System.out.println("Enter Y2: ");
				Y2 = dd.nextInt();
			} while (Y2 < 1 || Y2 > 10000 || (Y1 == Y2));

		}
		dd.close();
 		String window=Integer.toString(X1)+","+Integer.toString(Y1)+","+Integer.toString(X2)+","+Integer.toString(Y2);
 		
		conf.set("window", window);
		
		Job job = new Job(conf);
		job.setJobName("query1");  
		job.setJarByClass(query1.class);
		Path pointsInputPath = new Path(args[0]); 
  	  	Path rectInputPath = new Path(args[1]); 
  	  	Path outputPath = new Path(args[2]);
  	  	MultipleInputs.addInputPath(job, pointsInputPath,
  	            TextInputFormat.class, PointsMap.class);
  	  	MultipleInputs.addInputPath(job, rectInputPath,
  	            TextInputFormat.class, RectangleMap.class);
        job.setReducerClass(Reduce.class);
        FileOutputFormat.setOutputPath(job, outputPath); 
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);           
  	  	job.setOutputKeyClass(Text.class);
  	  	job.setOutputValueClass(Text.class); 
        boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
 }




	public static void main(String[] args) throws Exception  
    	{  
        int res = ToolRunner.run(new Configuration(), new query1(), args);  
        System.exit(res);  
    	}  







}