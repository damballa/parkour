package parkour.hadoop.input;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import org.apache.hadoop.conf.Configuration;
import parkour.hadoop.interfaces.IInputSplit;

public class MultiplexInputSplit extends ProxyInputSplit {
  private static class Vars {
    private static final String NS = "parkour.mapreduce.input.multiplex";
    private static final Var inputSplit = RT.var(NS, "input-split*");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  public MultiplexInputSplit() {
    super();
  }

  public MultiplexInputSplit(Configuration conf, Object... args) {
    super(conf, args);
  }

  IInputSplit createSplit(Configuration conf, Object... args) {
    return (IInputSplit) Vars.inputSplit.applyTo(RT.cons(conf, RT.seq(args)));
  }
}
