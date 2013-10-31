package parkour.hadoop;

import java.io.IOException;

import clojure.lang.IDeref;
import clojure.lang.RT;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class WrapperRecordReader<K, V> extends RecordReader<K, V> {
  protected RecordReader<K, V> rr;

  public WrapperRecordReader(RecordReader<K, V> rr) {
    this.rr = rr;
  }

  @Override
  public void close() throws IOException {
    rr.close();
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return rr.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return rr.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return rr.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    rr.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return rr.nextKeyValue();
  }
}
