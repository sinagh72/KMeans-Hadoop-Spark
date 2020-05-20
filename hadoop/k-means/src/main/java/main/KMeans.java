package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans {
	public static class KMeansMapper extends Mapper<LongWritable, Text, Text, DataPoint> {
		private ArrayList<DataPoint> centroids;

		public void setup(Context context) throws IOException, InterruptedException {
			this.centroids = Main.centroids;
		}

		// reuse Hadoop's Writable objects
		private final Text reducerKey = new Text();
		private final DataPoint reducerValue = new DataPoint();

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

	public static class KMeansCombiner extends Reducer<Text, DataPoint, Text, DataPoint> {

		public void setup(Context context) throws IOException, InterruptedException {
		}

		public void reduce(Text key, Iterable<DataPoint> values, Context context)
				throws IOException, InterruptedException {
			// output of combiner
			DataPoint out = new DataPoint();
			// build the unsorted list of data points
			List<DataPoint> dataPoints = new ArrayList<DataPoint>();
			values.forEach(dp -> dataPoints.add(DataPoint.copy(dp)));
			// sort the data points in memory
			Collections.sort(dataPoints);
			// aggregation of data points (vectors) in a same cluster
			ArrayList<Double> sumVectors = new ArrayList<Double>(
					Collections.nCopies(dataPoints.get(0).getVector().size(), (double) 0));

			//
			dataPoints.forEach(dp -> {
				out.addMinDistance(dp.getMinDistance());
				IntStream.range(0, dp.getVector().size()).mapToDouble(i -> dp.getVector().get(i) + sumVectors.get(i));
			});
			//
			out.setVector(sumVectors);
			context.write(key, out);
		}
	}

	public static class KMeansReducer extends Reducer<Text, DataPoint, Text, Text> {

		public void setup(Context context) throws IOException, InterruptedException {
		}

		public void reduce(Text key, Iterable<DataPoint> values, Context context)
				throws IOException, InterruptedException {
			// output of combiner
			DataPoint out = new DataPoint();
			// build the unsorted list of data points
			List<DataPoint> dataPoints = new ArrayList<DataPoint>();
			values.forEach(dp -> dataPoints.add(DataPoint.copy(dp)));
			// sort the data points in memory
			Collections.sort(dataPoints);
			// aggregation of data points (vectors) in a same cluster
			ArrayList<Double> sumVectors = new ArrayList<Double>(
					Collections.nCopies(dataPoints.get(0).getVector().size(), (double) 0));

			//
			dataPoints.forEach(dp -> {
				out.addMinDistance(dp.getMinDistance());
				IntStream.range(0, dp.getVector().size())
						.mapToDouble(i -> dp.getVector().get(i) / dataPoints.size() + sumVectors.get(i));
			});
			//

			out.setVector(sumVectors);
			context.write(key, new Text(out.toString().replace("[", "").replace("]", "")));

		}
	}
}
