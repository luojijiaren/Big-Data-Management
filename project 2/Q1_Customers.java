import  java.io.*;
import java.util.Random;

class P
{
	public static void main(String[] args) 
	{
		
		long[][] P=new long[1000][1000];
		Random rand=new Random();
		for (int i=1;i<=1000;i++)
		{
			int r= rand.nextInt(1000);
			P[i][r]=1;
			System.out.println(P[i][r]);

		}	
	
	}
}