package parkour.hadoop;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;


public class Partitioner
    extends org.apache.hadoop.mapreduce.Partitioner
    implements Configurable {

private static final String VAR_KEY = "parkour.partitioner.var";
private static final String OPTIONS_KEY = "parkour.partitioner.options";

private Configuration conf = null;
private IFn f = null;

public Configuration
getConf() {
    return this.conf;
}

public void
setConf(Configuration conf) {
    this.conf = conf;
    String[] fqname = conf.get(VAR_KEY).split("/", 2);
    Object options = RT.readString(conf.get(OPTIONS_KEY, "{}"));
    if (fqname[0].startsWith("#'")) fqname[0] = fqname[0].substring(2);
    RT.var("clojure.core", "require").invoke(Symbol.intern(fqname[0]));
    this.f = (IFn) RT.var(fqname[0], fqname[1]).invoke(conf, options);
}

@Override
public int
getPartition(Object key, Object val, int numPartitions) {
    if (f instanceof IFn.OOLL)
        return (int) ((IFn.OOLL) f).invokePrim(key, val, numPartitions);
    return RT.intCast(f.invoke(key, val, numPartitions));
}

}
