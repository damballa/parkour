package parkour.hadoop;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface IOutputFormat<K, V> {
  RecordWriter<K, V> getRecordWriter(TaskAttemptContext context);
  void checkOutputSpecs(JobContext context);
  OutputCommitter getOutputCommitter(TaskAttemptContext context);
}
