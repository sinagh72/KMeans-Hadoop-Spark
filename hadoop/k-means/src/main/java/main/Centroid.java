package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Centroid {

	public static ArrayList<DataPoint> run(String path, FileSystem hdfs, int k, int n)
			throws IllegalArgumentException, IOException {
		FSDataInputStream inputStream = hdfs.open(new Path(path));

//		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//		int lines = 0;
//		while (reader.readLine() != null)
//			lines++;
//		reader.close();

		ArrayList<Integer> randoms = new ArrayList<>(k);
		Random r = new Random();
		for (int i = 0; i < k; i++) {
			int temp = r.nextInt(n);
			while (randoms.contains(temp))
				temp = r.nextInt(n);
			randoms.add(temp);
		}
		// ascending sort
		Collections.sort(randoms);
		Collections.reverse(randoms);
		//
		ArrayList<DataPoint> centroids = new ArrayList<>();
		// read each line of the input file
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		int lines = 1;
		int counter = 0;
		String line = reader.readLine();
		while (line != null) {
			String[] tokens;
			if (randoms.get(counter) == lines) {
				tokens = line.trim().split(",");
				DataPoint p = new DataPoint();
				p.set(tokens);
				centroids.add(p);
				counter++;
				if (counter == k)
					break;
			}
			line = reader.readLine();
		}
		reader.close();

		//
		return centroids;

	}

}
