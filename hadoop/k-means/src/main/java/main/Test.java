package main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

public class Test {

	public static void main(String[] args) throws IOException {
		int n = 10;
		int m = 2;
		int k = 2;
		ArrayList<DataPoint> centroids = new ArrayList<>();
		for (int i = 0; i < k; i++) {
			centroids.add(new DataPoint());
		}
		ArrayList<DataPoint> p1 = new ArrayList<>();
		ArrayList<DataPoint> p2 = new ArrayList<>();
		BufferedReader reader2 = new BufferedReader(new InputStreamReader(new FileInputStream("file.txt")));

		for (int i = 0; i < k; i++) {
			String line = reader2.readLine();
			DataPoint p = new DataPoint();
			String[] vals = line.trim().split(";");
			System.out.println(Arrays.toString(vals));
			p.set(vals[1].split(","));
			centroids.set(Integer.parseInt(vals[0]), p);
		}
		reader2.close();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("k-means_input.txt")));
		for (int i = 0; i < 2000; i++) {
			String[] tokens = reader.readLine().trim().split(",");
			if (Integer.parseInt(tokens[0]) > n - 1) {
				break;
			}
			String vector = tokens[1];
			for (int z = 2; z < m + 1; z++) {
				vector += "," + tokens[z];
			}
			DataPoint p = new DataPoint();
			p.set(vector.split(","));
			if (p.findNearestCentroid(centroids) == 1) {
				p1.add(p);
			} else {
				p2.add(p);
			}
		}
		ArrayList<Double> sumVectors1 = new ArrayList<Double>(
				Collections.nCopies(p1.get(0).getVector().size(), (double) 0));
		p1.forEach(dp -> {
			for (int j = 0; j < dp.getVector().size(); j++) {
				sumVectors1.set(j, dp.getVector().get(j) / p1.size() + sumVectors1.get(j));
			}
			IntStream.range(0, dp.getVector().size())
					.mapToDouble(b -> sumVectors1.set(b, dp.getVector().get(b) + sumVectors1.get(b)));
		});
		ArrayList<Double> sumVectors2 = new ArrayList<Double>(
				Collections.nCopies(p1.get(0).getVector().size(), (double) 0));
		p2.forEach(dp -> {
			for (int j = 0; j < dp.getVector().size(); j++) {
				sumVectors2.set(j, dp.getVector().get(j) / p2.size() + sumVectors2.get(j));
			}
			IntStream.range(0, dp.getVector().size())
					.mapToDouble(b -> sumVectors2.set(b, dp.getVector().get(b) + sumVectors2.get(b)));
		});
		System.out.println(sumVectors1);
		System.out.println(sumVectors2);
		reader.close();
		
	}

}
