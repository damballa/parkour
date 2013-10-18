package parkour.hadoop;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class ProxyRecordWriter<K, V> extends RecordWriter<K, V> {
  protected final IRecordWriter<K, V> irw;

  ProxyRecordWriter(IRecordWriter<K, V> irw) {
    this.irw = irw;
  }

  @Override
  public void write(K key, V value) {
    irw.write(key, value);
  }

  @Override
  public void close(TaskAttemptContext context) {
    irw.close(context);
  }
}
