package Q1_2;
import  java.io.*;
import  java.util.*;


public class Q1_Transactions_test {
    public static void main(String[] args) {
        FileOutputStream tranfile = null;
	    PrintStream tranfp=null;        

	try {
		File newfile = new File("Transactions_test");
		newfile.createNewFile();
        tranfile=new FileOutputStream(newfile) ;
        tranfp=new PrintStream(tranfile);


            for (int i=1;i<=50000;i++){
				Transactions tr= new Transactions();
                String string = Integer.toString(tr.transId) + "," + Integer.toString(tr.custId)+ "," + Float.toString(tr.transTotal)
                         + "," + Integer.toString(tr.transNumItems) + "," + tr.transDesc + "\n";
                tranfp.print(string);
                }

		 } 
		  catch(Exception e) { 
            System.out.println(e.getMessage());
           

        } 



        

        

    }
}
