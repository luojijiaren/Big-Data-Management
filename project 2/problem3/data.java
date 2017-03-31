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

public class data {
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Random rand = new Random();

    try{
        PrintWriter writer = new PrintWriter("P.txt", "UTF-8");
        for(int i =1;i<=10500000;i++)
        {

            int j = rand.nextInt(10001);
	   		int k = rand.nextInt(10001);
            writer.println(k+","+j);
        }
        writer.close();
        } catch (IOException e) {
            // do nothing
        }

    }
}
