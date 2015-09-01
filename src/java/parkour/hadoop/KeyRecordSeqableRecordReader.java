package parkour.hadoop;

import java.io.Closeable;
import java.io.IOException;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.RT;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KeyRecordSeqableRecordReader
    extends RecordReader<Object, NullWritable> {

  private final IFn f;
  private Object rs;
  private ISeq coll;
  private Object key;
  private int total;
  private int current;

  public KeyRecordSeqableRecordReader(IFn f) {
    this.f = f;
  }

  @Override
  public void close() throws IOException {
    if (rs instanceof Closeable) ((Closeable) rs).close();
  }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public NullWritable getCurrentValue()
      throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (coll == null) return 1.0f;
    return (float) Math.max((double) current / (double) total, 1.0);
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.rs = f.invoke(split, context);
    this.coll = RT.seq(rs);
    this.key = null;
    this.total = RT.count(rs);
    this.current = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (coll == null) return false;
    this.key = coll.first();
    ++current;
    coll = coll.next();
    return true;
  }
}
