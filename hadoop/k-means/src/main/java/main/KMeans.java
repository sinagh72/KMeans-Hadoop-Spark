package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans {
	public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final ArrayList<DataPoint> centroids = new ArrayList<>();

		private int m;
		private int n;

		public void setup(Context context) throws IOException, InterruptedException {
			int k = context.getConfiguration().getInt("k-means.cluster.number", 1);
			m = context.getConfiguration().getInt("k-means.columns", 1);
			n = context.getConfiguration().getInt("k-means.rows", 1);
			String path = context.getConfiguration().getStrings("k-means.centroid.path")[0];
			Centroid.readCentroids(centroids, k, path, FileSystem.get(context.getConfiguration()));
		}

		// reuse Hadoop's Writable objects
		private final Text reducerKey = new Text();
		private final Text reducerValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String record = value.toString().trim();
			if (record == null || record.length() == 0)
				return;
			String[] tokens = record.split(",");
			if (Integer.parseInt(tokens[0]) > n - 1)
				return;
			String vector = tokens[1];
			for (int i = 2; i < m + 1; i++) {
				vector += "," + tokens[i];
			}
			DataPoint dp = new DataPoint();
			dp.set(vector.split(","));
			reducerValue.set(vector + ";" + 1);
			reducerKey.set(dp.findNearestCentroid(this.centroids) + ""); // set the name as key
			context.write(reducerKey, reducerValue);
		}
	}

	public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
		private int m = 0;

		public void setup(Context context) throws IOException, InterruptedException {
			this.m = context.getConfiguration().getInt("k-means.columns", 1);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// build the unsorted list of data points
			double n = 0;
			double[] sumVectors = new double[m];
			for (Text txt : values) {
				String[] vals = txt.toString().trim().split(";");
				n += Double.parseDouble(vals[1]);
				int i = 0;
				for (String val : vals[0].trim().split(",")) {
					sumVectors[i] += Double.parseDouble(val);
					i++;
				}
			}
			context.write(key, new Text(Arrays.toString(sumVectors).replace("[", "").replace("]", "") + ";" + n));
		}
	}

	public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
		private int m = 0;

		public void setup(Context context) throws IOException, InterruptedException {
			this.m = context.getConfiguration().getInt("k-means.columns", 1);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// build the unsorted list of data points
			double n = 0;
			double[] sumVectors = new double[m];
			for (Text txt : values) {
				String[] vals = txt.toString().trim().split(";");
				n += Double.parseDouble(vals[1]);
				int i = 0;
				for (String val : vals[0].trim().split(",")) {
					sumVectors[i] += Double.parseDouble(val);
					i++;
				}
			}

			for (int j = 0; j < sumVectors.length; j++) {
				sumVectors[j] /= n;
			}

//			List<DataPoint> dataPoints = new ArrayList<DataPoint>();
//			for (DataPoint dp : values) {
//				dataPoints.add(DataPoint.copy(dp));
//			}
//			ArrayList<Double> sumVectors = new ArrayList<Double>(
//					Collections.nCopies(dataPoints.get(0).getVector().size(), (double) 0));
//
//			dataPoints.forEach(dp -> {
//				for (int j = 0; j < dp.getVector().size(); j++) {
//					sumVectors.set(j, dp.getVector().get(j) / dataPoints.size() + sumVectors.get(j));
//				}
//			});
//
			context.write(new Text(key + ";"), new Text(Arrays.toString(sumVectors).replace("[", "").replace("]", "")));

		}
	}
}
