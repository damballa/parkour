package parkour.hadoop;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;

public class AvroKeyGroupingComparator<T>
    extends Configured
    implements RawComparator<AvroKey<T>> {

  public static final String
    CONF_GROUPING_SCHEMA = "parkour.avro.grouping.schema";

  private GenericData mDataModel;
  private Schema mSchema;

  public static void setGroupingSchema(Job job, Schema schema) {
    job.setGroupingComparatorClass(AvroKeyGroupingComparator.class);
    job.getConfiguration().set(CONF_GROUPING_SCHEMA, schema.toString());
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) return;
    mDataModel = AvroSerialization.createDataModel(conf);
    String schemaString = conf.get(CONF_GROUPING_SCHEMA);
    if (schemaString != null) mSchema = new Parser().parse(schemaString);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return BinaryData.compare(b1, s1, b2, s2, mSchema);
  }

  @Override
  public int compare(AvroKey<T> x, AvroKey<T> y) {
    return mDataModel.compare(x.datum(), y.datum(), mSchema);
  }
}
