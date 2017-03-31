/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package project1;
import java.io.PrintWriter;
import java.util.Random;
import java.lang.String;
import java.io.IOException;
/**
 *
 * @author chenglezhang
 */

public class kpoints {
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
    
    if (args.length != 1) {
      System.err.println("specify k");
      System.exit(2);
    }
        Random rand = new Random();
    int k = Integer.parseInt(args[0]);
    try{
        PrintWriter writer = new PrintWriter("k.txt", "UTF-8");
        for(int i =0;i<k;i++)
        {

            int j = rand.nextInt(10001);
	   		int l = rand.nextInt(10001);
            writer.println(l+","+j);
        }
        writer.close();
        } catch (IOException e) {
            // do nothing
        }

    }
}
