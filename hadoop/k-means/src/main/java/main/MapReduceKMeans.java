package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceKMeans {
	private ArrayList<DataPoint> centroids;
	private static boolean valid = true;

	public static class KMeansMapper extends Mapper<LongWritable, Text, Text, DataPoint> {
		private int k;
		private int n;

		public void setup(Context context) throws IOException, InterruptedException {
			if (valid) {
				valid = false;
				this.k = context.getConfiguration().getInt("map.reduce.centroid.numbers", 1);
				this.n = context.getConfiguration().getInt("map.reduce.centroid.numbers", 1);
				Random rand = new Random();
				rand.nextInt(n);
				for (int i = 0; i < k; i++) {
					float temp = r.nextInt(dataset.length);
					while (centroids.containsKey(temp))// check for not selecting one data twice, no repetitious
						temp = r.nextInt(dataset.length);
					centroids.put(temp, new ArrayList<Float>()); // consider a list of data for each centeroid
				}
				
			}
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
			DataPoint p = new DataPoint(tokens);
			reducerKey.set(tokens[0]); // set the name as key
			reducerValue.set(date.getTime(), Double.parseDouble(tokens[2]));
			context.write(reducerKey, reducerValue);
		}
	}

	public static class KMeansReducer extends Reducer<Text, DataPoint, Text, Text> {
		private int windowSize;

		public void setup(Context context) throws IOException, InterruptedException {
			this.windowSize = context.getConfiguration().getInt("map.reduce.matrix.column.size", 100);
		}

		public void reduce(Text key, Iterable<DataPoint> values, Context context)
				throws IOException, InterruptedException {
			// build the unsorted list of timeseries
			List<TimeSeriesData> timeseries = new ArrayList<TimeSeriesData>();
			for (TimeSeriesData tsData : values) {
				timeseries.add(TimeSeriesData.copy(tsData));
			}

			// sort the timeseries data in memory
			Collections.sort(timeseries);

			// apply moving average algorithm to sorted timeseries
			Text outputValue = new Text(); // reuse object

			double sum = 0.0d;
			for (int i = 0; i < windowSize - 1; i++)
				sum += timeseries.get(i).getValue();

			for (int i = windowSize - 1; i < timeseries.size(); i++) {
				sum += timeseries.get(i).getValue();

				double movingAverage = sum / windowSize;
				long timestamp = timeseries.get(i).getTimestamp();
				outputValue.set(DateUtil.getDateAsString(timestamp) + ", " + movingAverage);
				context.write(key, outputValue);

				sum -= timeseries.get(i - windowSize + 1).getValue();
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: k-means <k> <input> <output>");
			System.exit(1);
		}
		System.out.println("args[0]: <k>=" + otherArgs[0]);
		System.out.println("args[1]: <input>=" + otherArgs[1]);
		System.out.println("args[2]: <output>=" + otherArgs[2]);
		
		Job job = Job.getInstance(conf, "MapReduceKMeans");
		job.setJarByClass(MapReduceKMeans.class);

		// set mapper/reducer
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);

		// define mapper's output key-value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataPoint.class);

		// define reducer's output key-value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set window size for moving average calculation
		int k = Integer.parseInt(otherArgs[0]);
		job.getConfiguration().setInt("map.reduce.centroid.numbers", k);
		

		// define I/O
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
