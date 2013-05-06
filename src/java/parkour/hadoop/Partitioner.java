package parkour.hadoop;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;


public class Partitioner
    extends org.apache.hadoop.mapreduce.Partitioner
    implements Configurable {

private Configuration conf = null;
private IFn f = null;

public Configuration
getConf() {
    return this.conf;
}

public void
setConf(Configuration conf) {
    this.conf = conf;
    String[] fqname = conf.get("parkour.partitioner").split("/", 2);
    RT.var("clojure.core", "require").invoke(Symbol.intern(fqname[0]));
    this.f = (IFn) RT.var(fqname[0], fqname[1]).invoke(conf);
}

@Override
public int
getPartition(Object key, Object val, int numPartitions) {
    return RT.intCast(this.f.invoke(key, val, numPartitions));
}

}
