package problem1;

import java.util.*;
import java.io.*;

public class createData {
	static Random random = new Random();
	public static void createPoint(){
		float x=0,y = 0;
		File csv = new File("points.csv");
		int n = 6000000;
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
			for (int i = 0; i < n; i++) {
			   x=random.nextFloat()*10000;
			   y=random.nextFloat()*10000;
			   while(x>10000||x<0){
				   x=random.nextFloat();
			   }
			   while(y>10000||y<0){
				   y=random.nextFloat();
			   }
				bw.write(x + "," + y);
				bw.newLine();
			}
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void createRectrangle(){
		float x=0,y=0;
		float w=0,h=0;
		File csv = new File("rectangle.csv");
		int n = 2500000;
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
			for (int i = 0; i < n; i++) {
				w=random.nextFloat()*5;
				h=random.nextFloat()*20;
				while(w<0||w>5){
					w=random.nextFloat();
				}
				while(h<0||h>20){
					h=random.nextFloat();
				}
				x=random.nextFloat()*10000;
				y=random.nextFloat()*10000;
				while(x<0||x>(10000-w)){
					x=random.nextFloat()*10000;
				}
				while(y>10000||y<h){
					y=random.nextFloat()*10000;
				}
				
				bw.write("r"+ (i+1) + "," +  x+ "," + y + "," + w+ "," + h);
				bw.newLine();
			}
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] args) {
		createPoint();
		createRectrangle();
	}
	
}
