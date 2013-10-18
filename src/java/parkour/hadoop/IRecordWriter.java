package parkour.hadoop;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface IRecordWriter<K, V> {
  void write(K key, V value);
  void close(TaskAttemptContext context);
}
