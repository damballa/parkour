package parkour.hadoop;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ClojureAvroKeyValueInputFormat<K, V>
    extends FileInputFormat<AvroKey<K>, AvroValue<V>> {

  @Override
  public RecordReader<AvroKey<K>, AvroValue<V>>
    createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    Configuration conf = context.getConfiguration();
    Schema ks = AvroJob.getInputKeySchema(conf);
    Schema vs = AvroJob.getInputValueSchema(conf);
    return new ClojureAvroKeyValueRecordReader<K, V>(ks, ks);
  }
}
