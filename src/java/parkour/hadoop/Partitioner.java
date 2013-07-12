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
private static final String ARGS_KEY = "parkour.partitioner.args";

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
    if (fqname[0].startsWith("#'")) fqname[0] = fqname[0].substring(2);
    RT.var("clojure.core", "require").invoke(Symbol.intern(fqname[0]));
    IFn tvar = RT.var(fqname[0], fqname[1]);
    String argsEDN = conf.get(ARGS_KEY);
    this.f = (argsEDN == null)
        ? (IFn) tvar.invoke(conf)
        : (IFn) tvar.applyTo(RT.cons(conf, RT.readString(argsEDN)));
}

@Override
public int
getPartition(Object key, Object val, int numPartitions) {
    if (f instanceof IFn.OOLL)
        return (int) ((IFn.OOLL) f).invokePrim(key, val, numPartitions);
    return RT.intCast(f.invoke(key, val, numPartitions));
}

}
