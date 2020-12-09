package part2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements WritableComparable<Pair> {
  private String key;
  private String value;

  public Pair() {};

  public Pair(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "(" + key + ", " + value + ')';
  }

  public int compareTo(Pair o) {
    if(this == null || o == null)
      return -1;
    int result = this.key.compareTo(o.getKey());
    return result == 0 ? this.value.compareTo(o.getValue()) : result;
  }

  public void write(DataOutput dataOutput) throws IOException { }

  public void readFields(DataInput dataInput) throws IOException { }
}
