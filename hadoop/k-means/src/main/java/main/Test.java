package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.IntStream;

import org.apache.kerby.util.SysUtil;

public class Test {
	public static void main(String[] args) throws IOException {
		DataPoint p1 = new DataPoint();
		p1.addVectorElement(3);
		p1.addVectorElement(4);

		DataPoint p2 = new DataPoint();
		p2.addVectorElement(4);
		p2.addVectorElement(3);

		System.out.println(p1.distance(p2, 2));

	}

}
