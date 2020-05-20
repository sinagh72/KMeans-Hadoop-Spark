package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SingleNodeKMeans {
	public static void main(String[] args) {
		// num of cluster
		int k = 3;
		// data set
		float[] dataset = new float[] { 1, 2, 3, 4, 5, 21, 8, 9, 55, 2221, 10, 11, 44, 223, 332, 221, 423, 229, 224 };
		// centroid of the k means (mu)
		Map<Float, ArrayList<Float>> centroids = new HashMap<Float, ArrayList<Float>>(k);
		// clone
		Map<Float, ArrayList<Float>> newCentroids = new HashMap<Float, ArrayList<Float>>(k);
		// random generator
		Random r = new Random();
		for (int i = 0; i < k; i++) {
			float temp = r.nextInt(dataset.length);
			while (centroids.containsKey(temp))// check for not selecting one data twice, no repetitious
				temp = r.nextInt(dataset.length);
			centroids.put(temp, new ArrayList<Float>()); // consider a list of data for each centeroid
		}
		int counter = 5;
		while (counter > 0) {// number of times that the procedure will loop! on the algorithm there is a
								// stop condition but I used loop
			counter--;

			for (int i = 0; i < dataset.length; i++) {// for each data in data set
				float min = Integer.MAX_VALUE;
				float key = -1;
				for (float j : centroids.keySet()) {// finding the min centroid
					float dist = distance(j, dataset[i]);
					if (min > dist) {
						min = dist;
						key = j;
					}
				}
				centroids.get(key).add(dataset[i]);// add the data into the centeroid list
			}
			if (counter != 0) {
				newCentroids.clear();
				centroids.forEach((e, v) -> {// recalculating the centeroid
					newCentroids.put(sum(v) / v.size(), new ArrayList<>());
				});
				centroids.clear();
				centroids.putAll(newCentroids);
			}
		}
		centroids.forEach((e, v) -> {
			System.out.println("centroid: " + e + " | dataset: " + v);
		});

	}

	// measure the distance
	private static float distance(float i, float point) {
		return Math.abs(i - point);
	}

	// summation of a list element
	private static float sum(ArrayList<Float> array) {
		int sum = 0;
		for (float i : array) {
			sum += i;
		}
		if (sum == 0)// avoid 0/0
			sum++;
		return sum;
	}

}
