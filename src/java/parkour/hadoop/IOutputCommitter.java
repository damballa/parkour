package parkour.hadoop;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public interface IOutputCommitter {
  void setupJob(JobContext context);
  void commitJob(JobContext context);
  void abortJob(JobContext context, JobStatus.State state);
  void setupTask(TaskAttemptContext context);
  boolean needsTaskCommit(TaskAttemptContext context);
  void commitTask(TaskAttemptContext context);
  void abortTask(TaskAttemptContext context);
}
