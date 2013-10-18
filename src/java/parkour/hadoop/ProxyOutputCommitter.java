package parkour.hadoop;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ProxyOutputCommitter extends OutputCommitter {
  protected final IOutputCommitter ioc;

  ProxyOutputCommitter(IOutputCommitter ioc) {
    this.ioc = ioc;
  }

  @Override
  public void setupJob(JobContext context) {
    ioc.setupJob(context);
  }

  @Override
  public void commitJob(JobContext context) {
    ioc.commitJob(context);
  }

  @Override
  @Deprecated
  public void cleanupJob(JobContext context) {
    commitJob(context);
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state) {
    ioc.abortJob(context, state);
  }

  @Override
  public void setupTask(TaskAttemptContext context) {
    ioc.setupTask(context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) {
    return ioc.needsTaskCommit(context);
  }

  @Override
  public void commitTask(TaskAttemptContext context) {
    ioc.commitTask(context);
  }

  @Override
  public void abortTask(TaskAttemptContext context) {
    ioc.abortTask(context);
  }
}
