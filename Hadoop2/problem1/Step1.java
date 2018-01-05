package problem1; /**
 * Created by Yihao on 2017/2/21.
 */

import java.io.*;

public class Step1 {

        public static void main(String[] args)  throws IOException {

            FileOutputStream cf = null;
            FileOutputStream tf = null;
            PrintStream cfp = null;
            PrintStream tfp = null;

            try {
                System.out.println("Starting process...");


                // create points file that is 100MB
                File file = new File("/Users/yihaozhou/Documents/IdeaProjects/ds503project2/data/P_test.data");
                if (file.createNewFile()){
                    System.out.println("File is created!");
                }else{
                    System.out.println("File already exists.");
                }
                cf = new FileOutputStream("/Users/yihaozhou/Documents/IdeaProjects/ds503project2/data/P_test.data");
                cfp = new PrintStream(cf);
                while (!file.exists() || file.length() < (1 * 1024 * 1024)) {
                    P point = new P();
                    String string = Integer.toString(point.id) + "," + Float.toString(point.x)  + "," + Float.toString(point.y) + "\n";
                    cfp.print(string);
                    System.out.println("Add points: " + point.id);
                }

                // create points file that is 100MB
                File file2 = new File("/Users/yihaozhou/Documents/IdeaProjects/ds503project2/data/R_test.data");
                if (file2.createNewFile()){
                    System.out.println("File is created!");
                }else{
                    System.out.println("File already exists.");
                }
                tf = new FileOutputStream("/Users/yihaozhou/Documents/IdeaProjects/ds503project2/data/R_test.data");
                tfp = new PrintStream(tf);
                while (!file2.exists() || file2.length() < (1 * 1024 * 1024)) {
                    R rec = new R();
                    String string = Integer.toString(rec.id) + "," + Float.toString(rec.x)  + "," + Float.toString(rec.y)
                            + "," + Float.toString(rec.w)  + "," + Float.toString(rec.h) + "\n";
                    tfp.print(string);
                    System.out.println("Add recs: " + rec.id);
                }

            } finally {
                if (cf != null) {
                    cfp.close();
                    cf.close();
                }
                if ( tf != null) {
                    tfp.close();
                    tf.close();
                }
            }


        }
}
