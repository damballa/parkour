package parkour.hadoop;

import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.Symbol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class Mappers {
  private static class Base extends Mapper {
    private static class Vars {
      private static final String NS = "parkour.mapreduce.tasks";
      private static final Var mapperRun = RT.var(NS, "mapper-run");
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
      Vars.mapperRun.invoke(id, context);
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
