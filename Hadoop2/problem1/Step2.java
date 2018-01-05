package problem1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Step2 extends Configured implements Tool {

    public static class MapperSetup extends Mapper<LongWritable, Text, Text, Text> {
        public static int xMin, xMid, xMax, yMin, yMid, yMax;

        public void setup(Context context) throws IOException, InterruptedException {
            String[] windows = context.getConfiguration().get("window").split(",");
            xMin = Integer.parseInt(windows[0]);
            xMax = Integer.parseInt(windows[1]);
            xMid = (xMin + xMax) / 2;
            yMin = Integer.parseInt(windows[2]);
            yMax = Integer.parseInt(windows[3]);
            yMid = (yMin + yMax) /2;
        }
    }


    public static class MapperPoints extends MapperSetup {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            float x = Float.parseFloat(line[1]);
            float y = Float.parseFloat(line[2]);
            int region = 0;

            if (x>xMin && x<xMid && y>yMin && y<yMid) {
                region = 1;
            }
            else if (x>=xMid && x<xMax && y>yMin && y<yMid) {
                region = 2;
            }
            else if (x>xMin && x<xMid && y>=yMid && y<yMax) {
                region = 3;
            }
            else if (x>=xMid && x<xMax && y>=yMid && y<yMax){
                region = 4;
            }

            if (region != 0 )
                context.write(new Text(Integer.toString(region)), new Text(line[1] + ',' + line[2]));

        }
    }

    public static class MapperRects extends MapperSetup{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            int id = Integer.parseInt(line[0]);
            float x = Float.parseFloat(line[1]);
            float y = Float.parseFloat(line[2]);
            float w = Float.parseFloat(line[3]);
            float h = Float.parseFloat(line[4]);

            // region 1
            if (doOverlap(x, y, x+w, y+h, xMin, xMid, yMin, yMid))
                context.write(new Text("1"), new Text(id+","+x+","+y+","+w+","+h));

            // region 2
            if (doOverlap(x, y, x+w, y+h, xMid, xMax, yMin, yMid))
                context.write(new Text("2"), new Text(id+","+x+","+y+","+w+","+h));

            // region 3
            if (doOverlap(x, y, x+w, y+h, xMin, xMid, yMid, yMax))
                context.write(new Text("3"), new Text(id+","+x+","+y+","+w+","+h));

            // region 4
            if (doOverlap(x, y, x+w, y+h, xMid, xMax, yMid, yMax))
                context.write(new Text("4"), new Text(id+","+x+","+y+","+w+","+h));
        }

        private boolean doOverlap(float tx, float ty, float bx, float by, int xmin, int xmax, int ymin, int ymax) {
            if (xmin > bx || tx > xmax)
                return false;

            if (ymax < ty || by < ymin)
                return false;

            return true;
        }
    }

    public static class ReducerJoin extends Reducer<Text, Text, Text, Text> {
        private List<String> points = new ArrayList<String>();
        private List<String> rects = new ArrayList<String>();
        Text key = new Text();
        Text output = new Text();

        public void reduce(Text cId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] line = val.toString().split(",");
                if (line.length == 2)
                    points.add(val.toString());
                else
                    rects.add(val.toString());
            }

            for (String p : points) {
                String[] point = p.split(",");
                float px = Float.parseFloat(point[0]);
                float py = Float.parseFloat(point[1]);
                for (String r : rects) {
                    String[] rect = r.split(",");
                    int id = Integer.parseInt(rect[0]);
                    float rx = Float.parseFloat(rect[1]);
                    float ry = Float.parseFloat(rect[2]);
                    float rw = Float.parseFloat(rect[3]);
                    float rh = Float.parseFloat(rect[4]);
                    if (px>=rx && px<=(rx+rw) && py>=ry && py<=(ry+rh)) {
                        key.set("r" + id);
                        output.set("(" + px + "," + py + ")");
                        context.write(key, output);
                    }
                }
            }

            points.clear();
            rects.clear();
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        if (args.length == 4)
            conf.set("window", args[3]);
        else
            conf.set("window", "0,10000,0,10000");

        Job job = new Job(conf, "Spatial Join");
        job.setJarByClass(Step2.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperPoints.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperRects.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(ReducerJoin.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return (job.waitForCompletion(true) ? 0 : 1);

    }

    public static void main(String[] args) throws Exception {
        int ecode = ToolRunner.run(new Step2(), args);
        System.exit(ecode);
    }

}