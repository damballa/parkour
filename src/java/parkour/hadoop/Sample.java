package parkour.hadoop;

import java.io.IOException;

import clojure.lang.IDeref;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class Sample {
  private static class Vars {
    private static final String NS = "parkour.remote.sample";
    private static final Var inputFormat = RT.var(NS, "input-format");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  public static class InputFormat<K, V> extends ProxyInputFormat<K, V> {
    @SuppressWarnings("unchecked")
    public InputFormat() {
      super((IInputFormat) Vars.inputFormat.invoke());
    }
  }
}
