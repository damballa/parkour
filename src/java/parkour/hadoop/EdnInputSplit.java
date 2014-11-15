package parkour.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import clojure.lang.IDeref;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class EdnInputSplit
    extends InputSplit implements IDeref, Writable, Configurable {

  private static class Vars {
    private static final String NS = "parkour.cser";
    private static final Var readString = RT.var(NS, "read-string");
    private static final Var prStr = RT.var(NS, "pr-str");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  private static final String NS = "parkour.mapreduce";
  private static final Keyword LENGTH = Keyword.intern(NS, "length");
  private static final Keyword LOCATIONS = Keyword.intern(NS, "locations");

  private Configuration conf;
  private Object value;

  public EdnInputSplit() {
    this.conf = null;
    this.value = null;
  }

  public EdnInputSplit(Object value) {
    this.conf = null;
    this.value = value;
  }

  public EdnInputSplit(Configuration conf, Object value) {
    this.conf = conf;
    this.value = value;
  }

  @Override
  public long getLength() {
    Object length = LENGTH.invoke(value);
    if (length == null) return 0;
    return RT.longCast(length);
  }

  @Override
  public String[] getLocations() {
    Object locations = LOCATIONS.invoke(value);
    if (locations instanceof String[]) return (String[]) locations;
    return (String[]) RT.seqToTypedArray(String.class, RT.seq(locations));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.value = Vars.readString.invoke(conf, Text.readString(in));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Object value1 = RT.dissoc(value, LOCATIONS);
    Text.writeString(out, (String) Vars.prStr.invoke(conf, value1));
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Object deref() {
    return value;
  }
}
