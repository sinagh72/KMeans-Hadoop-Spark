package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err.println("Usage: k-means <k> <rows> <columns> <threshold> <input> <output>");
			System.exit(1);
		}
		System.out.println("args[0]: <k>=" + otherArgs[0]);
		System.out.println("args[1]: <rows>=" + otherArgs[1]);
		System.out.println("args[2]: <colaumns>=" + otherArgs[2]);
		System.out.println("args[2]: <threshold>=" + otherArgs[3]);
		System.out.println("args[3]: <input>=" + otherArgs[4]);
		System.out.println("args[4]: <output>=" + otherArgs[5]);
		// set the number of clusters
		int k = Integer.parseInt(otherArgs[0]);
		// set the rows
		int n = Integer.parseInt(otherArgs[1]);
		// set the columns
		int m = Integer.parseInt(otherArgs[2]);
		// threshold
		double threshold = Double.parseDouble(otherArgs[3]);
		// selecting the random k points
		FileSystem hdfs = FileSystem.get(conf);
		//
		if (hdfs.exists(new Path(otherArgs[5] + "/pre")))
			hdfs.delete(new Path(otherArgs[5] + "/pre"), true);
		if (hdfs.exists(new Path(otherArgs[5] + "/new")))
			hdfs.delete(new Path(otherArgs[5] + "/new"), true);
		if (hdfs.exists(new Path(otherArgs[5] + "/temp")))
			hdfs.delete(new Path(otherArgs[5] + "/temp"), true);
		//
		Centroid.run(conf, k, n, m, otherArgs[4], otherArgs[5] + "/pre");
		//
		FileUtil.copy(hdfs, new Path(otherArgs[5] + "/pre"), hdfs, new Path(otherArgs[5] + "/temp"), false, conf);
		long time = System.currentTimeMillis();
		boolean isChanged = true;
		int counter = 0;
		while (isChanged && counter < 10) {
			Job kMeans = Job.getInstance(conf, "MapReduceKMeans");
			kMeans.setJarByClass(Main.class);
			// set mapper/combiner/reducer
			kMeans.setMapperClass(KMeans.KMeansMapper.class);
//			kMeans.setCombinerClass(KMeans.KMeansCombiner.class);
			kMeans.setReducerClass(KMeans.KMeansReducer.class);
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
			kMeans.getConfiguration().setStrings("k-means.centroid.path", otherArgs[5] + "/temp/part-r-00000");
			// pass the rows
			kMeans.getConfiguration().setInt("k-means.rows", n);
			// pass the columns
			kMeans.getConfiguration().setInt("k-means.columns", m);
			// define I/O
			FileInputFormat.addInputPath(kMeans, new Path(otherArgs[4]));
			FileOutputFormat.setOutputPath(kMeans, new Path(otherArgs[5] + "/new"));

			kMeans.setInputFormatClass(TextInputFormat.class);
			kMeans.setOutputFormatClass(TextOutputFormat.class);

			int out = kMeans.waitForCompletion(true) ? 0 : 1;
			//
			isChanged = false;
			// check if the centroids values has been changed or not

			BufferedReader reader = new BufferedReader(
					new InputStreamReader(hdfs.open(new Path(otherArgs[5] + "/temp/part-r-00000"))));
			BufferedReader reader2 = new BufferedReader(
					new InputStreamReader(hdfs.open(new Path(otherArgs[5] + "/new/part-r-00000"))));
			for (int i = 0; i < k; i++) {
				String r1 = reader.readLine();
				String r2 = reader2.readLine();
				DataPoint p1 = new DataPoint();
				p1.set(r1.trim().split(";")[1].split(","));
				DataPoint p2 = new DataPoint();
				p2.set(r2.trim().split(";")[1].split(","));
				if (p1.distance(p2, 2) > m * Math.pow(threshold, 2)) {
					isChanged = true;
				}
				System.out.println("====================");
				System.out.println("r1: " + r1);
				System.out.println("r2: " + r2);
				System.out.println("====================");
			}
			if (isChanged) {
				System.out.println("changed");
				hdfs.delete(new Path(otherArgs[5] + "/temp"), true);
				hdfs.rename(new Path(otherArgs[5] + "/new"), new Path(otherArgs[5] + "/temp"));
				hdfs.delete(new Path(otherArgs[5] + "/new"), true);
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
