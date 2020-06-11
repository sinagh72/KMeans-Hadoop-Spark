package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 9) {
			System.err.println(
					"Usage: k-means <k> <rows> <columns> <threshold> <maxItr> <reducers> <combiner> <input> <output>");
			System.exit(1);
		}
		System.out.println("args[0]: <k>=" + otherArgs[0]);
		System.out.println("args[1]: <rows>=" + otherArgs[1]);
		System.out.println("args[2]: <colaumns>=" + otherArgs[2]);
		System.out.println("args[3]: <threshold>=" + otherArgs[3]);
		System.out.println("args[4]: <mamxItr>=" + otherArgs[4]);
		System.out.println("args[5]: <reducers>=" + otherArgs[5]);
		System.out.println("args[6]: <combiner>=" + otherArgs[6]);
		System.out.println("args[7]: <input>=" + otherArgs[7]);
		System.out.println("args[8]: <output>=" + otherArgs[8]);
		// set the number of clusters
		int k = Integer.parseInt(otherArgs[0]);
		// set the rows
		int n = Integer.parseInt(otherArgs[1]);
		// set the columns
		int m = Integer.parseInt(otherArgs[2]);
		// threshold
		double threshold = Double.parseDouble(otherArgs[3]);
		// maxItr
		long maxItr = Long.parseLong(otherArgs[4]);
		// reducers
		int reducers = Integer.parseInt(otherArgs[5]);
		// combiner
		boolean combiner = Boolean.parseBoolean(otherArgs[6]);

		FileSystem hdfs = FileSystem.get(conf);
		//
		if (hdfs.exists(new Path(otherArgs[8] + "/pre")))
			hdfs.delete(new Path(otherArgs[8] + "/pre"), true);
		if (hdfs.exists(new Path(otherArgs[8] + "/new")))
			hdfs.delete(new Path(otherArgs[8] + "/new"), true);
		if (hdfs.exists(new Path(otherArgs[8] + "/temp")))
			hdfs.delete(new Path(otherArgs[8] + "/temp"), true);
		// selecting the random k points Centroid mapReduce
		Centroid.run(conf, k, n, m, otherArgs[7], otherArgs[8] + "/pre");
		//
		FileUtil.copy(hdfs, new Path(otherArgs[8] + "/pre"), hdfs, new Path(otherArgs[8] + "/temp"), false, conf);
		// K-means mapReduce
		KMeans.run(maxItr, conf, combiner, reducers, k, m, n, threshold, otherArgs[7], otherArgs[8]);

	}

}
