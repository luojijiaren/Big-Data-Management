import java.io.*
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFCell;


public class Customers
{
	
     public String randomWord() {
		 int length = rand.nextInt(10) + 11;
		 String word = "";
		 for ((int i=0;i<length;i++) {
			 word +=(char) randomChar();
		 }
		 return word;
		 }

     public static void main(String[] args){
		try {
			String filename = "C:/Users/fzhan/Documents/GitHub/Big-Data-Management/Project 1/NewExcelFiles.xls";
			HSSFWorkbook workbook = new HSSFWorkbook();
			HSSFSheet sheet =workbook.createsheet("FirstSheet");
			
			HSSFRow rowhead=sheet.createRow((short)0);
			rowhead.createCell(0).setCellValue(
			rowhead.createCell(1).setCellValue(
			rowhead.createCell(2).setCellValue(
			rowhead.createCell(3).setCellValue(
	     
	     
	     
                Dataset data= new DefaultDataset();
                for (int i=0;i<10;i++) {
                     Instance tmpInstance = InstanceTools.randomInstance(25);
                     data.add(temInstance);
                     }
	           	System.out.println(data);

                int ID = 1:50000;
                char UserName = randomWord();
                int Age = rand.nextInt(60)+10;
                int CountryCOde=rand.nextInt(10);
                float Salary=Math.random()*9900+100;
                sb.append(all

	}
}
