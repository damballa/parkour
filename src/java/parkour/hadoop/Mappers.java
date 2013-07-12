package parkour.hadoop;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import org.apache.hadoop.conf.Configuration;

public class Mappers {

private static class Base extends org.apache.hadoop.mapreduce.Mapper {
  private static final String CONF_KEY_BASE = "parkour.mapper.";

  private final String varKey;
  private final String argsKey;
  private final long id;

  public
  Base() {
      super();
      this.id = Long.parseLong(getClass().getName().split("\\$_", 2)[1]);
      String confKey = CONF_KEY_BASE + Long.toString(this.id);
      this.varKey = confKey + ".var";
      this.argsKey = confKey + ".args";
  }

  public void
  run(Context context) {
      Configuration conf = context.getConfiguration();
      String[] fqname = conf.get(varKey).split("/", 2);
      if (fqname[0].startsWith("#'")) fqname[0] = fqname[0].substring(2);
      RT.var("clojure.core", "require").invoke(Symbol.intern(fqname[0]));
      IFn tvar = RT.var(fqname[0], fqname[1]);
      String argsEDN = conf.get(argsKey);
      IFn f = (argsEDN == null)
          ? (IFn) tvar.invoke(conf)
          : (IFn) tvar.applyTo(RT.cons(conf, RT.readString(argsEDN)));
      f.invoke(context);
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
