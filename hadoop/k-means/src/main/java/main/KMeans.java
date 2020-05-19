package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class KMeans {

	public static float[] get_dataset(){
		return new float[] { 1, 2, 3, 4, 5, 21, 8, 9, 55, 2221, 10, 11, 44, 223, 332, 221, 423, 229, 224 };
	}

	public static void init_centroids(Map<Float, ArrayList<Float>> centroids, int length){
		// random generator
		Random r = new Random();
		for (int i = 0; i < k; i++) {
			float temp = r.nextInt(length);
			while (centroids.containsKey(dataset[temp]))// check for not selecting one data twice, no repetitious; Search dataset[temp] value if exists instead of just temp
				temp = r.nextInt(length);
			centroids.put(dataset[temp], new ArrayList<Float>()); // consider a list of data for each centeroid; register the value in dataset[temp] instead of just temp
		}
	}

	public static int compute_error(Map<Float, ArrayList<Float>> centroids, float[] dataset){
		return 5;
	}

	public static void compute_new_centroids(Map<Float, ArrayList<Float>> centroids, Map<Float, ArrayList<Float>> newCentroids){
		newCentroids.clear();
		centroids.forEach((e, v) -> {// recalculating the centroid
			newCentroids.put(sum(v) / v.size(), new ArrayList<>());
		});
		centroids.clear();
		centroids.putAll(newCentroids);
	}

	public static void compute_centroids(Map<Float, ArrayList<Float>> centroids){
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
	}

	public static void main(String[] args) {
		// num of cluster
		int k = 3;
		// data set
		float[] dataset = get_data();
		// centroid of the k means (mu)
		Map<Float, ArrayList<Float>> centroids = new HashMap<Float, ArrayList<Float>>(k);
		// clone
		Map<Float, ArrayList<Float>> newCentroids = new HashMap<Float, ArrayList<Float>>(k);
		
		init_centroids(centroids, dataset.length);

		int counter = compute_error(centroids, dataset);
		
		while (counter > 0) {// number of times that the procedure will loop! on the algorithm there is a
								// stop condition but I used loop
			counter--;

			compute_centroids(centroids, dataset);

			if (counter != 0) {
				compute_new_centroids(centroids, newCentroids);
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
