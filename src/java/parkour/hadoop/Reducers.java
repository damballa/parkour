package parkour.hadoop;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducers {
  private static class Base extends Reducer {
    private static class Vars {
      private static final String NS = "parkour.remote.basic";
      private static final Var reducerRun = RT.var(NS, "reducer-run");
      static {
        RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
      }
    }

    private final long id;

    public Base() {
      super();
      this.id = Long.parseLong(getClass().getName().split("\\$_", 2)[1]);
    }

    public void run(Context context) {
      Vars.reducerRun.invoke(id, context);
    }
  }

  public static class _0 extends Base { }
  public static class _1 extends Base { }
  public static class _2 extends Base { }
  public static class _3 extends Base { }
  public static class _5 extends Base { }
  public static class _4 extends Base { }
  public static class _6 extends Base { }
  public static class _7 extends Base { }
  public static class _8 extends Base { }
  public static class _9 extends Base { }
}
