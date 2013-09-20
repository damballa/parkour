package parkour.hadoop;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class Partitioner
  extends org.apache.hadoop.mapreduce.Partitioner
  implements Configurable {

  private static class Vars {
    private static final String NS = "parkour.mapreduce.tasks";
    private static final Var
      partitionerSetConf = RT.var(NS, "partitioner-set-conf");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  private Configuration conf = null;
  private IFn f = null;

  public Configuration getConf() {
    return this.conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.f = (IFn) Vars.partitionerSetConf.invoke(conf);
  }

  @Override
  public int getPartition(Object key, Object val, int numPartitions) {
    if (f instanceof IFn.OOLL)
      return (int) ((IFn.OOLL) f).invokePrim(key, val, numPartitions);
    return RT.intCast(f.invoke(key, val, numPartitions));
  }
}
