package parkour.hadoop.input;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import parkour.hadoop.interfaces.IInputFormat;

public class MultiplexInputFormat<K, V> extends ProxyInputFormat<K, V> {
  private static class Vars {
    private static final String NS = "parkour.mapreduce.input.multiplex";
    private static final Var inputFormat = RT.var(NS, "input-format");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  @SuppressWarnings("unchecked")
  public MultiplexInputFormat() {
    super((IInputFormat) Vars.inputFormat.invoke());
  }
}
