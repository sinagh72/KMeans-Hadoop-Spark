package main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Stream;

public class Test {
	public static void main(String[] args) throws IOException {
//		DataPoint[] points = new DataPoint[] {};
		ArrayList<Integer> d = new ArrayList<Integer>();
		d.add(1);
		d.add(4);
		d.add(5);
//		ArrayList<Integer> z = new ArrayList<Integer>(d);
//		d.set(0, -1);
//		d.add(23);
//		System.out.println(z.toString().replace("[", "").replace("]", ""));
		Random r = new Random();

		d.forEach(i -> {
			String[] line;
			try (Stream<String> lines = Files.lines(Paths.get("file.txt"))) {
				line = lines.skip(i).findFirst().get().trim().split(",");
				System.out.println(line);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});

	}

}
