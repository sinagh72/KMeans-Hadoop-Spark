package main;

import java.io.BufferedReader;
import java.io.InputStreamReader;

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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: k-means <k> <rows> <columns> <input> <output>");
			System.exit(1);
		}
		System.out.println("args[0]: <k>=" + otherArgs[0]);
		System.out.println("args[1]: <rows>=" + otherArgs[1]);
		System.out.println("args[2]: <columns>=" + otherArgs[2]);
		System.out.println("args[3]: <input>=" + otherArgs[3]);
		System.out.println("args[4]: <output>=" + otherArgs[4]);
		// set the number of clusters
		int k = Integer.parseInt(otherArgs[0]);
		// set the rows
		int n = Integer.parseInt(otherArgs[1]);
		// set the columns
		int m = Integer.parseInt(otherArgs[2]);
		// selecting the random k points
		FileSystem hdfs = FileSystem.get(conf);
		//
		Centroid.run(conf, k, n, m, otherArgs[3], otherArgs[4] + "/pre");
		//
//		ArrayList<DataPoint> centroids = Centroid.readCentroids(k, otherArgs[4] + "/pre/part-r-00000", hdfs);
		//

		boolean isChanged = true;
		int counter = 5;
//		while (isChanged && counter > 0) {
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
			kMeans.getConfiguration().setStrings("k-means.centroid.path", otherArgs[4] + "/pre/part-r-00000");
			// pass the rows
			kMeans.getConfiguration().setInt("k-means.rows", n);
			// pass the columns
			kMeans.getConfiguration().setInt("k-means.columns", m);
			// define I/O
			FileInputFormat.addInputPath(kMeans, new Path(otherArgs[3]));
			FileOutputFormat.setOutputPath(kMeans, new Path(otherArgs[4] + "/new"));

			kMeans.setInputFormatClass(TextInputFormat.class);
			kMeans.setOutputFormatClass(TextOutputFormat.class);

			int out = kMeans.waitForCompletion(true) ? 0 : 1;
			//
			isChanged = false;
			// check if the centroids values has been changed or not

			BufferedReader reader = new BufferedReader(
					new InputStreamReader(hdfs.open(new Path(otherArgs[4] + "/pre/part-r-00000"))));
			BufferedReader reader2 = new BufferedReader(
					new InputStreamReader(hdfs.open(new Path(otherArgs[4] + "/new/part-r-00000"))));
			for (int i = 0; i < k; i++) {
				if (!reader.readLine().equals(reader2.readLine()))
					isChanged = true;

			}
			if (isChanged) {
//				System.out.println("changed");
//				hdfs.delete(new Path(otherArgs[4] + "/pre"), true);
//				hdfs.rename(new Path(otherArgs[4] + "/new"), new Path(otherArgs[4] + "/pre"));
//				hdfs.delete(new Path(otherArgs[4] + "/new"), true);
			}
			reader.close();
			reader2.close();
			counter--;
//		}
	}

}
