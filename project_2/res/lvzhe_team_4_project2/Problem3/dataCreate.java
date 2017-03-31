package project2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class dataCreate {
	
	private int randomInt(int min, int max) {
		Random random = new Random();
		int r = random.nextInt(max) % (max - min + 1) + min;
		return r;
	}
	
	private float randomFloat(int min, int max) {
		Random random = new Random();
		float r = random.nextFloat() * max;
		while (r < min) {
			r = random.nextFloat() * max;
			
		}
		return r;
	}
	
	public void datasetFile() {
		int min = 0;
		int max = 10000;
		int count = 6000000;
		File csv = new File("C:/Users/QiWang/Desktop/dataset1.csv");
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
			for ( int i = 0; i < count; i++) {
//				int x = randomInt(min, max);
//				int y = randomInt(min, max);
				float x = randomFloat(min, max);
				float y = randomFloat(min, max);
//				bw.write(String.valueOf(i));
//				bw.write(",");
				bw.write(String.valueOf(x));
				bw.write(",");
				bw.write(String.valueOf(y));
				bw.newLine();
			}
			bw.close();
		} catch(FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}