package step1;
import java.io.*;
public class step1 {

	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub

        FileOutputStream cf = null;
        PrintStream cfp = null;


        try {
            File file = new File("C:/Users/fzhan/Documents/GitHub/Big-Data-Management/project_3/data/P.data");
            cf = new FileOutputStream("C:/Users/fzhan/Documents/GitHub/Big-Data-Management/project_3/data/P.data");
            cfp = new PrintStream(cf);
            while (file.length() < (1 * 1024 * 1024)) {
                P point = new P();
                String string = Integer.toString(point.id) + "," + Float.toString(point.x)  + "," + Float.toString(point.y) + "\n";
                cfp.print(string);
                System.out.println("Add points: " + point.id);
            }



        } finally {
            if (cf != null) {
                cfp.close();
                cf.close();
            }

        }

	}

}
