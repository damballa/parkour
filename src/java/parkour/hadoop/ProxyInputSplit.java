package parkour.hadoop;

import java.io.DataInput;
import java.io.DataOutput;

import clojure.lang.IDeref;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public abstract class ProxyInputSplit
    extends InputSplit implements IDeref, Writable, Configurable {

  private Configuration conf;
  private IInputSplit iis;

  public ProxyInputSplit() {
    this.iis = null;
  }

  public ProxyInputSplit(Configuration conf, Object... args) {
    this.conf = conf;
    this.iis = createSplit(conf, args);
  }

  @Override
  public long getLength() {
    return iis.getLength();
  }

  @Override
  public String[] getLocations() {
    return iis.getLocations();
  }

  @Override
  public void readFields(DataInput in) {
    this.iis = iis.readSplit(in);
  }

  @Override
  public void write(DataOutput out) {
    iis.write(out);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (iis == null)
      this.iis = createSplit(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Object deref() {
    return iis.deref();
  }

  abstract IInputSplit
    createSplit(Configuration conf, Object... args);
}
