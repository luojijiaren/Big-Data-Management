package Q1_2;

import java.util.Random;
import java.io.*;

public class Transactions  
{

	static int seq=0;
	int transId;
	int custId;
	float transTotal;
	int transNumItems;
	String transDesc;
	Random rand = new Random();


	public Transactions()  
	{
		this.seq ++;
		this.transId = this.seq;
		this.custId=this.getCustId();
		this.transTotal=this.getTransTotal();
		this.transNumItems=this.getTransNumItems();
		this.transDesc=this.getTransDesc();
	}

	private int getCustId() 
	{
		
		int r1= rand.nextInt(49999)+1;
		return r1;
	}	
	
		private float getTransTotal() 
	{
		
	    float r2=rand.nextFloat()*990+10;
		return r2;
	}

	private int getTransNumItems() 
	{

		int r3= rand.nextInt(9)+1;
		return r3;
	}


	private String getTransDesc() {
		 String word = "";
		 int length = rand.nextInt(10) + 11;
		 char[] chars = "abcdefghijklmnopqrstuvwlyz".toCharArray();
		 StringBuilder sb=new StringBuilder();
		 for (int i=0;i<length;i++) {
			 char c = chars[rand.nextInt(chars.length)];
			 sb.append(c);
		     }
		 word = sb.toString();
		 return word;
	}





}
