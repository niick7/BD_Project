package part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Pair implements Writable, WritableComparable<Pair> {
  private Text key;
  private Text value;

  public Pair() {
    this.key = new Text("");
    this.value = new Text("");
  };

  public Pair(Text key, Text value) {
    this.setKey(key);
    this.setValue(value);
  }

  public Text getKey() {
    return key;
  }

  public void setKey(Text key) {
    this.key = key;
  }

  public Text getValue() {
    return value;
  }

  public void setValue(Text value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "(" + key + ", " + value + ')';
  }

  @Override
  public int compareTo(Pair o) {
    if(this == null || o == null)
      return -1;
    int result = this.key.compareTo(o.getKey());
    return result == 0 ? this.value.compareTo(o.getValue()) : result;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    value.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key.readFields(in);
    value.readFields(in);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }
}