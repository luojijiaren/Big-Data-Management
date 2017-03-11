import  java.io.*;
import  org.apache.poi.hssf.usermodel.HSSFSheet;
import  org.apache.poi.hssf.usermodel.HSSFWorkbook;
import  org.apache.poi.hssf.usermodel.HSSFRow;
import  org.apache.poi.hssf.usermodel.HSSFCell;
import java.util.Random;

public class CreateExlFile{

    public String randomWord() {
		 Random randlen=new Random();
		 int length = randlen.nextInt(10) + 11;
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
            String filename = "C:/Users/fzhan/Documents/NewExcelFile.xls" ;
            HSSFWorkbook workbook = new HSSFWorkbook();
			//;
			Random rand=new Random();
			
            HSSFSheet sheet = workbook.createSheet("FirstSheet");  

            HSSFRow rowhead = sheet.createRow((short)0);
            rowhead.createCell(0).setCellValue("No.");
            rowhead.createCell(1).setCellValue("Name");
            rowhead.createCell(2).setCellValue("Address");
            rowhead.createCell(3).setCellValue("Email");
			
			CreateExlFile test = new CreateExlFile();

            for (int i=1;i<=100;i++){
			    HSSFRow row = sheet.createRow((short)i);
                row.createCell(0).setCellValue(i);
				int r1= rand.nextInt(60)+10;
                row.createCell(1).setCellValue(r1);
			    String r2= test.randomWord();
                row.createCell(2).setCellValue(r2);
				float r3=rand.nextFloat()*9900+100;
                row.createCell(3).setCellValue(r3);
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