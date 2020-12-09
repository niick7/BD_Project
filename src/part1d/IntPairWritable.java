package part1d;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IntPairWritable implements WritableComparable<IntPairWritable> {
	private int first = 0;
	private int second = 0;

	// Set the left and right values.
	public IntPairWritable (int left, int right) {
		first = left;
		second = right;
	}
	
	// Set the left and right values.
	public void set(int left, int right) {
		first = left;
		second = right;
	}

	
	public int getFirst() {
		return first;
	}

	public int getSecond() {
		return second;
	}
	// Read the two integers. 
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public int hashCode() {
		return Objects.hash(first, second);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof IntPairWritable) {
			IntPairWritable r = (IntPairWritable) right;
		return r.first == first && r.second == second;
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(IntPairWritable tp) {
	    int cmp = (first - tp.first);
	    if (cmp != 0) {
	        return cmp;
	    }
	    return (second - tp.second);
	}
}