package parkour.hadoop;

import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface IInputFormat<K, V> {
  List<InputSplit> getSplits(JobContext context);
  RecordReader<K, V> createRecordReader(
      InputSplit split, TaskAttemptContext context);
}
