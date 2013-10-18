package parkour.hadoop;

import clojure.lang.IDeref;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import org.apache.hadoop.conf.Configuration;

public class Dux {
  private static class Vars {
    private static final String NS = "parkour.remote.dux";
    private static final Var outputFormat = RT.var(NS, "output-format*");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  public static class OutputFormat<K, V> extends ProxyOutputFormat<K, V> {
    public OutputFormat() {
      super();
    }

    public OutputFormat(Configuration conf, Object... args) {
      super(conf, args);
    }

    @SuppressWarnings("unchecked")
    IOutputFormat<K, V> createOutputFormat(Configuration conf, Object... args) {
      return (IOutputFormat<K, V>)
        Vars.outputFormat.applyTo(RT.cons(conf, RT.seq(args)));
    }
  }

  public static class RecordWriter<K, V> extends ProxyRecordWriter<K, V> {
    public RecordWriter(IRecordWriter<K, V> irw) {
      super(irw);
    }
  }

  public static class OutputCommitter
      extends ProxyOutputCommitter implements IDeref {
    public OutputCommitter(IOutputCommitter ioc) {
      super(ioc);
    }

    public Object deref() {
      return ((IDeref) ioc).deref();
    }
  }
}
