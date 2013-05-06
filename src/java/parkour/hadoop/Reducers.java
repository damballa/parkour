package parkour.hadoop;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import org.apache.hadoop.conf.Configuration;

public class Reducers {

private static class Base extends org.apache.hadoop.mapreduce.Reducer {
  private static final String CONF_KEY_BASE = "parkour.reducer.";

  private String
  getConfKey() {
      return CONF_KEY_BASE + getClass().getName().split("\\$_", 2)[1];
  }

  public void
  run(Context context) {
      Configuration conf = context.getConfiguration();
      String[] fqname = conf.get(getConfKey()).split("/", 2);
      RT.var("clojure.core", "require").invoke(Symbol.intern(fqname[0]));
      ((IFn) RT.var(fqname[0], fqname[1]).invoke(conf)).invoke(context);
  }
}

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
