package main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import org.apache.hadoop.io.Text;

public class Test {

	public static void main(String[] args) throws IOException {
//		int n = 12;
//		int m = 7;
//		int k = 3;
//		ArrayList<DataPoint> centroids = new ArrayList<>();
//		for (int i = 0; i < k; i++) {
//			centroids.add(new DataPoint());
//		}
//		ArrayList<String> p1 = new ArrayList<>();
//		ArrayList<String> p2 = new ArrayList<>();
//		ArrayList<String> p3 = new ArrayList<>();
//		BufferedReader reader2 = new BufferedReader(new InputStreamReader(new FileInputStream("file.txt")));
//
//		for (int i = 0; i < k; i++) {
//			String line = reader2.readLine();
//			DataPoint p = new DataPoint();
//			String[] vals = line.trim().split(";");
//			p.set(vals[1].split(","));
//			centroids.set(Integer.parseInt(vals[0]), p);
//		}
//		reader2.close();
//		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("k-means_input.txt")));
//		for (int i = 0; i < 2000; i++) {
//			String[] tokens = reader.readLine().trim().split(",");
//			if (Integer.parseInt(tokens[0]) > n - 1) {
//				break;
//			}
//			String vector = tokens[1];
//			for (int z = 2; z < m + 1; z++) {
//				vector += "," + tokens[z];
//			}
//			DataPoint p = new DataPoint();
//			p.set(vector.split(","));
//			int index = p.findNearestCentroid(centroids);
//			if (index == 0) {
//				p1.add(p.toString() + ";" + 1);
//			} else if (index == 1) {
//				p2.add(p.toString() + ";" + 1);
//			} else if (index == 2) {
//				p3.add(p.toString() + ";" + 1);
//			}
//		}
//		reader.close();
//
//		double np = 0;
//		double[] sumVectors = new double[m];
//		for (String txt : p3) {
//			
//			String[] vals = txt.toString().trim().split(";");
//			np += Double.parseDouble(vals[1]);
//			System.out.println(vals[1]);
//			int i = 0;
//			for (String val : vals[0].trim().split(",")) {
//				sumVectors[i] += Double.parseDouble(val);
//				i++;
//			}
//		}
//		System.out.println(n);
//		for (int j = 0; j < sumVectors.length; j++) {
//			sumVectors[j] /= np;
//		}
//		System.out.println(Arrays.toString(sumVectors));
		DataPoint p1 = new DataPoint();
		String r1 = "0;  96922, 169551, 317846";
		String r2 = "5;  385115.81967213115, 165570.30327868852, 71451.4918032787";
		p1.set(r1.trim().split(";")[1].split(","));
		DataPoint p2 = new DataPoint();
		p2.set(r2.trim().split(";")[1].split(","));
		//
//		p1.set(new String("96922, 169551, 317846").split(","));
//		p2.set(new String("385115.81967213115, 165570.30327868852, 71451.4918032787").split(","));
		//
		System.out.println(p1.distance(p2, 2));

	}

}
