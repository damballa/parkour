package parkour.hadoop;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface IRecordReader<K, V> {
  void close();
  K getCurrentKey();
  V getCurrentValue();
  float getProgress();
  IRecordReader<K, V> initialize(InputSplit split, TaskAttemptContext context);
  boolean nextKeyValue();
}
