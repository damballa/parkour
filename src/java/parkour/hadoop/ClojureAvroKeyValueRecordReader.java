package parkour.hadoop;

import java.io.IOException;

import clojure.lang.Indexed;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroRecordReaderBase;

public class ClojureAvroKeyValueRecordReader<K, V>
    extends AvroRecordReaderBase<AvroKey<K>, AvroValue<V>, Indexed> {
  private final AvroKey<K> key;
  private final AvroValue<V> val;

  public ClojureAvroKeyValueRecordReader(Schema ks, Schema vs) {
    super((vs == null || ks == null) ? null : AvroKeyValue.getSchema(ks, vs));
    key = new AvroKey<K>(null);
    val = new AvroValue<V>(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean nextp = super.nextKeyValue();
    if (nextp) {
      Indexed kv = getCurrentRecord();
      key.datum((K) kv.nth(0));
      val.datum((V) kv.nth(1));
    } else {
      key.datum(null);
      val.datum(null);
    }
    return nextp;
  }

  @Override
  public AvroKey<K> getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public AvroValue<V> getCurrentValue()
      throws IOException, InterruptedException {
    return val;
  }
}
