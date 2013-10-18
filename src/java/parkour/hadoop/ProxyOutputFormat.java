package parkour.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class ProxyOutputFormat<K, V>
    extends OutputFormat<K, V> implements Configurable {

  protected Configuration conf;
  protected IOutputFormat<K, V> iof;

  public ProxyOutputFormat() {
    this.conf = null;
    this.iof = null;
  }

  ProxyOutputFormat(Configuration conf, Object... args) {
    this.conf = conf;
    this.iof = createOutputFormat(conf, args);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (iof == null)
      this.iof = createOutputFormat(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) {
    return iof.getRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    iof.checkOutputSpecs(context);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return iof.getOutputCommitter(context);
  }

  abstract IOutputFormat<K, V>
    createOutputFormat(Configuration conf, Object... args);
}
