package part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StrPairWritable implements WritableComparable<StrPairWritable> {
  private Text key;
  private Text value;

  public StrPairWritable() {
    this.key = new Text("");
    this.value = new Text("");
  }

  public StrPairWritable(Text key, Text value) {
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
  public int compareTo(StrPairWritable o) {
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