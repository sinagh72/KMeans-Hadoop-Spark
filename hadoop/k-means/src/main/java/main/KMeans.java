package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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

			context.write(new Text(key + ";"), new Text(Arrays.toString(sumVectors).replace("[", "").replace("]", "")));

		}
	}

	public static void run(long maxItr, Configuration conf, boolean combiner, int reducers, int k, int m, int n,
			double threshold, String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		FileSystem hdfs = FileSystem.get(conf);
		long time = System.currentTimeMillis();
		boolean isChanged = true;
		int counter = 0;
		while (isChanged && counter < maxItr) {
			Job kMeans = Job.getInstance(conf, "MapReduceKMeans");
			kMeans.setJarByClass(Main.class);
			// set mapper/combiner/reducer
			kMeans.setMapperClass(KMeans.KMeansMapper.class);
			if (combiner)// the the combiner
				kMeans.setCombinerClass(KMeans.KMeansCombiner.class);
			kMeans.setReducerClass(KMeans.KMeansReducer.class);
			// set the number of reducers
			kMeans.setNumReduceTasks(reducers);
			// define mapper's output key-value
			kMeans.setMapOutputKeyClass(Text.class);
			kMeans.setMapOutputValueClass(Text.class);
			// define reducer's output key-value

			// define reducer's output key-value
			kMeans.setOutputKeyClass(Text.class);
			kMeans.setOutputValueClass(Text.class);

			// pass the number of cluster
			kMeans.getConfiguration().setInt("k-means.cluster.number", k);
			// pass the file of a selected centroids
			kMeans.getConfiguration().setStrings("k-means.centroid.path", outputPath + "/temp/part-r-00000");
			// pass the rows
			kMeans.getConfiguration().setInt("k-means.rows", n);
			// pass the columns
			kMeans.getConfiguration().setInt("k-means.columns", m);
			// define I/O
			FileInputFormat.addInputPath(kMeans, new Path(inputPath));
			FileOutputFormat.setOutputPath(kMeans, new Path(outputPath + "/new"));

			kMeans.setInputFormatClass(TextInputFormat.class);
			kMeans.setOutputFormatClass(TextOutputFormat.class);

			int out = kMeans.waitForCompletion(true) ? 0 : 1;
			System.out.println(out);
			//
			isChanged = false;
			// check if the centroids values has been changed or not

			BufferedReader reader = new BufferedReader(
					new InputStreamReader(hdfs.open(new Path(outputPath + "/temp/part-r-00000"))));
			BufferedReader reader2 = new BufferedReader(
					new InputStreamReader(hdfs.open(new Path(outputPath + "/new/part-r-00000"))));
			for (int i = 0; i < k; i++) {
				String r1 = reader.readLine();
				String r2 = reader2.readLine();
				if (r1 != null && r2 != null) {
					DataPoint p1 = new DataPoint();
					p1.set(r1.trim().split(";")[1].split(","));
					DataPoint p2 = new DataPoint();
					p2.set(r2.trim().split(";")[1].split(","));
					if (p1.distance(p2, 2) > threshold) {
						isChanged = true;
					}
				}
				System.out.println("====================");
				System.out.println("r1: " + r1);
				System.out.println("r2: " + r2);
				System.out.println("====================");
			}
			if (isChanged) {
				System.out.println("changed");
				hdfs.delete(new Path(outputPath + "/temp"), true);
				hdfs.rename(new Path(outputPath + "/new"), new Path(outputPath + "/temp"));
				hdfs.delete(new Path(outputPath + "/new"), true);
			}

			reader.close();
			reader2.close();
			counter++;
		}
		System.out.println("====================");
		System.out.println("time elapssed for convergance: " + (System.currentTimeMillis() - time));
		System.out.println("counter: " + counter);
		System.out.println("====================");
	}
}
