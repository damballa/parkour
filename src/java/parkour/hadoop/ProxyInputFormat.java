package parkour.hadoop;

import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class ProxyInputFormat<K, V> extends InputFormat<K, V> {
  private final IInputFormat<K, V> iif;

  ProxyInputFormat(IInputFormat<K, V> iif) {
    this.iif = iif;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    return iif.getSplits(context);
  }

  @Override
  public RecordReader<K, V> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return iif.createRecordReader(split, context);
  }
}
