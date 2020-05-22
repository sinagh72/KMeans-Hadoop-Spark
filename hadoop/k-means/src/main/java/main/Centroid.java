package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Centroid {

	public static class CentroidMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final Text reducerKey = new Text();
		private final Text reducerValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString().trim();
			if (record == null || record.length() == 0)
				return;
			String[] tokens = record.split(",");
			reducerKey.set(tokens[0]);
			reducerValue.set(record.substring(tokens[0].length() + 1));
			context.write(reducerKey, reducerValue);
		}
	}

	public static class CentroidReducer extends Reducer<Text, Text, IntWritable, Text> {
		private final ArrayList<Integer> randoms = new ArrayList<>();

		public void setup(Context context) throws IOException, InterruptedException {
			int k = context.getConfiguration().getInt("k-means.cluster.number", 1);
			int n = context.getConfiguration().getInt("k-means.rows", 1);
			// random obj
			Random r = new Random();
			// generating random numbers
			for (int i = 0; i < k; i++) {
				int temp = r.nextInt(n);
				while (this.randoms.contains(temp))
					temp = r.nextInt(n);
				this.randoms.add(temp);
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (this.randoms.contains(Integer.parseInt(key.toString()))) {
				context.write(new IntWritable(randoms.indexOf(Integer.parseInt(key.toString()))),
						values.iterator().next());
			}

		}
	}

	public static void run(Configuration conf, int k, int n, String input, String output)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Job centroidFinder = Job.getInstance(conf, "finding-centroids");
		centroidFinder.setJarByClass(Main.class);

		// set mapper/reducer
		centroidFinder.setMapperClass(Centroid.CentroidMapper.class);
		centroidFinder.setReducerClass(Centroid.CentroidReducer.class);

		// define mapper's output key-value
		centroidFinder.setMapOutputKeyClass(Text.class);
		centroidFinder.setMapOutputValueClass(Text.class);
		// define reducer's output key-value

		// define reducer's output key-value
		centroidFinder.setOutputKeyClass(IntWritable.class);
		centroidFinder.setOutputValueClass(Text.class);

		// pass the number of cluster
		centroidFinder.getConfiguration().setInt("k-means.cluster.number", k);
		// pass the size of a vector
		centroidFinder.getConfiguration().setInt("k-means.rows", n);

		// define I/O
		FileInputFormat.addInputPath(centroidFinder, new Path(input));
		FileOutputFormat.setOutputPath(centroidFinder, new Path(output));

		centroidFinder.setInputFormatClass(TextInputFormat.class);
		centroidFinder.setOutputFormatClass(TextOutputFormat.class);

		System.exit(centroidFinder.waitForCompletion(true) ? 0 : 1);
	}

	public static ArrayList<DataPoint> readCentroids(int k, String path, FileSystem hdfs)
			throws IllegalArgumentException, IOException {
		ArrayList<DataPoint> dp = new ArrayList<DataPoint>(k);
		BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(path))));
		String line = reader.readLine();
		while (line != null) {
			DataPoint p = new DataPoint();
			p.set(line.split("     ")[1].trim().split(","));
			dp.add(p);
		}
		reader.close();
		return dp;
	}

}
