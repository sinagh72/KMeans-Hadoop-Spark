package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	static ArrayList<DataPoint> centroids;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: k-means <k> <number of data points> <input> <data output> <centroid output>");
			System.exit(1);
		}
		System.out.println("args[0]: <k>=" + otherArgs[0]);
		System.out.println("args[1]: <number of data points>=" + otherArgs[1]);
		System.out.println("args[2]: <input>=" + otherArgs[2]);
		System.out.println("args[3]: <data output>=" + otherArgs[3]);
		System.out.println("args[3]: <centroid output>=" + otherArgs[4]);
		// set the number of clusters
		int k = Integer.parseInt(otherArgs[0]);
		// set the number of clusters
		int n = Integer.parseInt(otherArgs[1]);
		// selecting the random k points
		FileSystem hdfs = FileSystem.get(conf);
		centroids = Centroid.run(otherArgs[2], hdfs, k, n);
		//
		boolean isChanged = true;
		int counter = 1;

		while (isChanged && counter < Integer.MAX_VALUE) {

			Job job = Job.getInstance(conf, "MapReduceKMeans");
			job.setJarByClass(Main.class);

			// set mapper/combiner/reducer
			job.setMapperClass(KMeans.KMeansMapper.class);
			job.setCombinerClass(KMeans.KMeansCombiner.class);
			job.setReducerClass(KMeans.KMeansReducer.class);

			// define mapper's output key-value
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DataPoint.class);
			// define reducer's output key-value

			// define reducer's output key-value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// define I/O
			FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			//
			isChanged = false;
			// check if the centroids values has been changed or not
			BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(otherArgs[3]))));
			for (int i = 0; i < k; i++) {
				DataPoint p = new DataPoint();
				p.set(reader.readLine().trim().split(","));
				if (!centroids.contains(p)) {
					isChanged = true;
					// updating centroids
					centroids.remove(k);
					centroids.add(p);
				}
			}
			if (!isChanged)
				System.out.println("k-means terminated in round: " + counter + " with: " + centroids);

			reader.close();
			counter--;
		}
	}

}
