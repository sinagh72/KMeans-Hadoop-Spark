package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans {
	public static class KMeansMapper extends Mapper<LongWritable, Text, Text, DataPoint> {
		private ArrayList<DataPoint> centroids;
		private final Text reducerKey = new Text();
		private final DataPoint reducerValue = new DataPoint();

		public void setup(Context context) throws IOException, InterruptedException {
			int k = context.getConfiguration().getInt("k-means.cluster.number", 1);
			String path = context.getConfiguration().getStrings("k-means.centroid.path")[0];
			this.centroids = Centroid.readCentroids(k, path, FileSystem.get(context.getConfiguration()));
		}

		// reuse Hadoop's Writable objects

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String record = value.toString();
			if (record == null || record.length() == 0)
				return;
			String[] tokens = record.trim().split(",");
			reducerValue.set(tokens);
			reducerKey.set(reducerValue.findNearestCentroid(this.centroids) + ""); // set the name as key
			context.write(reducerKey, reducerValue);
		}
	}

	public static class KMeansCombiner extends Reducer<Text, DataPoint, Text, Text> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void reduce(Text key, Iterable<DataPoint> values, Context context)
				throws IOException, InterruptedException {
			// build the unsorted list of data points
			List<DataPoint> dataPoints = new ArrayList<DataPoint>();
			values.forEach(dp -> dataPoints.add(DataPoint.copy(dp)));
			// sort the data points in memory
//			Collections.sort(dataPoints);
			// aggregation of data points (vectors) in a same cluster
			ArrayList<Double> sumVectors = new ArrayList<Double>(
					Collections.nCopies(dataPoints.get(0).getVector().size(), (double) 0));

			//
			dataPoints.forEach(dp -> {
				IntStream.range(0, dp.getVector().size())
						.mapToDouble(i -> sumVectors.set(i, dp.getVector().get(i) + sumVectors.get(i)));
			});
			//
			context.write(key, new Text(sumVectors.toString() + ";" + (double) dataPoints.size()));
		}
	}

	public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
		private int col = 0;

		public void setup(Context context) throws IOException, InterruptedException {
			this.col = context.getConfiguration().getInt("k-means.vector.size", 1);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// build the unsorted list of data points
			double n = 0;
			double[] sumVectors = new double[col];
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				String[] vals = itr.next().toString().trim().split(";");
				n += Double.parseDouble(vals[1]);
				int i = 0;
				for (String val : vals[0].trim().replace("[", "").replace("]", "").split(",")) {
					sumVectors[i] += Double.parseDouble(val);
					i++;
				}
			}

			for (int j = 0; j < sumVectors.length; j++) {
				sumVectors[j] /= n;
			}
			// sort the data points in memory
//			Collections.sort(dataPoints);
			// aggregation of data points (vectors) in a same cluster

			//

//			dataPoints.forEach(dp -> {
//				out.addMinDistance(dp.getMinDistance());
//				IntStream.range(0, dp.getVector().size())
//						.mapToDouble(i -> dp.getVector().get(i) / dataPoints.size() + sumVectors.get(i));
//			});
//			//
//
//			out.setVector(sumVectors);
			context.write(key, new Text(Arrays.toString(sumVectors).toString().replace("[", "").replace("]", "")));

		}
	}
}
