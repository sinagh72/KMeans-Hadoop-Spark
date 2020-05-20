package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.IntStream;

import org.apache.hadoop.io.Writable;

public class DataPoint implements Writable, Comparable<DataPoint> {
	private ArrayList<Double> vector;
	private double minDistance = 0;

	public static DataPoint copy(final DataPoint dp) {
		return new DataPoint(dp.vector);
	}

	public DataPoint() {
		// TODO Auto-generated constructor stub
	}

	public DataPoint(final ArrayList<Double> vector) {
		this.vector = vector;
		// this.vector = vector.clone();
	}

	@Override
	public int compareTo(DataPoint that) {
		// TODO Auto-generated method stub
		if (this == that)
			return 1;
		if (this.minDistance > that.minDistance)
			return 1;
		if (this.minDistance < that.minDistance)
			return -1;
		if (this.minDistance == that.minDistance) {
			if (this.norm(2) > that.norm(2))
				return 1;
			if (this.norm(2) > that.norm(2))
				return -1;
			else
				return 0;
		}
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataPoint that = (DataPoint) obj;
		if (that.getVector().size() != this.getVector().size())
			return false;
		for (int i = 0; i < that.getVector().size(); i++) {
			if (that.getVector().get(i) != this.getVector().get(i))
				return false;
		}
		return true;

	}

	public double norm(int norm) {
		return Math.pow(Math.abs(vector.stream().mapToDouble(x -> Math.pow(x, norm)).sum()), (double) 1 / norm);
	}

	public double distance(DataPoint that, int norm) {
		return Math.pow(
				Math.pow(this.norm(norm), norm) + Math.pow(that.norm(norm), norm)
						- IntStream.range(0, this.vector.size())
								.mapToDouble(i -> 2 * this.vector.get(i) * that.vector.get(i)).sum(),
				(double) 1 / norm);
	}

	public void set(String[] tokens) {
		for (String str : tokens) {
			vector.add(Double.parseDouble(str));
		}
	}

	public int findNearestCentroid(ArrayList<DataPoint> centroids) {
		this.minDistance = Double.MAX_VALUE;
		int index = -1;
		for (DataPoint cent : centroids) {
			double dist = this.distance(cent, 2);
			if (dist < this.minDistance) {
				this.minDistance = dist;
				index = centroids.indexOf(cent);
			}
		}
		return index;
	}

	public double getMinDistance() {
		return minDistance;
	}

	public void addMinDistance(double minDistance) {
		this.minDistance += minDistance;
	}

	public ArrayList<Double> getVector() {
		return vector;
	}

	public void setVector(final ArrayList<Double> vector) {
		this.vector = vector;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

}
