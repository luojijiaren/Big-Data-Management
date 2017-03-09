package Q1_1;

import java.util.Random;
import java.io.*;

public class Customers  
{

	static int seq=0;
	int id;
	String name;
	int age;
	int countryCode;
	float salary;
	Random rand = new Random();


	public Customers()  
	{
		this.seq ++;
		this.id = this.seq;
		this.name=this.getName();
		this.age=this.getAge();
		this.countryCode=this.getCountryCode();
		this.salary=this.getSalary();
	}

	private String getName() {
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

	private int getAge() 
	{
		
		int r2= rand.nextInt(60)+10;
		return r2;
	}

	private int getCountryCode() 
	{

		int r3= rand.nextInt(9)+1;
		return r3;
	}

	private float getSalary() 
	{
		float r4=rand.nextFloat()*9900+100;
		return r4;
	}



}
