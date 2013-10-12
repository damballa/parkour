package parkour.hadoop.input;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import org.apache.hadoop.mapreduce.Mapper;

public class MultiplexMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2> {
  private static class Vars {
    private static final String NS = "parkour.remote.mux";
    private static final Var mapper = RT.var(NS, "mapper");
    static {
      RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
  }

  public void run(Context context) {
    ((IFn) Vars.mapper.invoke(context.getConfiguration())).invoke(context);
  }
}
