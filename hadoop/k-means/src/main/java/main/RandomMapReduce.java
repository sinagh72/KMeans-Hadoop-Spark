package main;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RandomMapReduce {

	public static class RandomMapper extends Mapper<Text, Text, Text, Text> {
		public void setup(Context context) {

		}

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class RandomReducer extends Reducer<Text, Text, Text, Text> {

		public void setup(Context context) throws IOException, InterruptedException {
//			int k = context.getConfiguration().getInt("map.reduce.cluster.numbers", 1);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		}

	}

}
