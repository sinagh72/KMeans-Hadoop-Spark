package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test {
	public static void main(String[] args) throws IOException {
//		DataPoint p1 = new DataPoint();
//		p1.addVectorElement(3);
//		p1.addVectorElement(4);
//
//		DataPoint p2 = new DataPoint();
//		p2.addVectorElement(4);
//		p2.addVectorElement(3);
//
//		System.out.println(p1.distance(p2, 2));
//
//		ArrayList<Double> d = new ArrayList<Double>();
//		d.add(2.3);
//		d.add(33d);
//		System.out.println(d.toString());
//
//		double[] f = new double[2];
//		f[0] += 1;
//		f[1] += 2;
//		
//		for (double q : f) {
//			q = q/4;
//		}
//		System.out.println(Arrays.toString(f));
//		String t = "538     2442,4798,2355,4278";
//		System.out.println(t.split("     ")[1]);
		ArrayList<DataPoint> d = new ArrayList<>(2);
		
		d.forEach(q -> System.out.println(2));

	}

}
