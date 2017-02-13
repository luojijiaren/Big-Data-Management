import  java.io.*;
import  org.apache.poi.xssf.usermodel.XSSFSheet;
import  org.apache.poi.xssf.usermodel.XSSFWorkbook;
import  org.apache.poi.xssf.usermodel.XSSFRow;
import  org.apache.poi.xssf.usermodel.XSSFCell;
import java.util.Random;

public class Q1_Transactions{

    public String randomWord() {
		 Random randlen=new Random();
		 int length = randlen.nextInt(20) + 31;
		 char[] chars = "abcdefghijklmnopqrstuvwlyz".toCharArray();
		 StringBuilder sb=new StringBuilder();
		 for (int i=0;i<length;i++) {
			 char c = chars[randlen.nextInt(chars.length)];
			 sb.append(c);
		     }
		 String word = sb.toString();
		 return word;
		 }
		 


    public static void main(String[]args) {
        try {
            String filename = "C:/Users/fzhan/Documents/GitHub/Big-Data-Management/Project 1/Transactions.xls" ;
            XSSFWorkbook workbook = new XSSFWorkbook();
			Random rand=new Random();
			
            XSSFSheet sheet = workbook.createSheet("FirstSheet");  

            XSSFRow rowhead = sheet.createRow((int)0);
            rowhead.createCell(0).setCellValue("TransID");
			rowhead.createCell(1).setCellValue("CustID");
            rowhead.createCell(2).setCellValue("TransTotal");
            rowhead.createCell(3).setCellValue("TransNumItems");
			rowhead.createCell(4).setCellValue("TransDesc");
			
			Q1_Transactions test = new Q1_Transactions();

            for (int i=1;i<=500000;i++){
			    XSSFRow row = sheet.createRow((int)i);
                row.createCell(0).setCellValue(i);
			    int r1= rand.nextInt(49999)+1;
                row.createCell(1).setCellValue(r1);                
				float r2=rand.nextFloat()*990+10;
                row.createCell(2).setCellValue(r2);
			    int r3= rand.nextInt(9)+1;
                row.createCell(3).setCellValue(r3);
                String r4= test.randomWord();
                row.createCell(4).setCellValue(r4);
                }


            FileOutputStream fileOut = new FileOutputStream(filename);
            workbook.write(fileOut);
            fileOut.close();
            System.out.println("Your excel file has been generated!");

        } catch ( Exception ex ) {
            System.out.println(ex);
        }
    }
}
