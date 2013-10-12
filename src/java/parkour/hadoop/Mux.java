package parkour.hadoop;

import java.io.IOException;

import clojure.lang.IDeref;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class Mux {
  private static class Vars {
    private static final String NS = "parkour.remote.mux";
    private static final Var inputFormat = RT.var(NS, "input-format");
    private static final Var inputSplit = RT.var(NS, "input-split*");
    private static final Var mapper = RT.var(NS, "mapper");
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

  public static class InputSplit extends ProxyInputSplit {
    public InputSplit() {
      super();
    }

    public InputSplit(Configuration conf, Object... args) {
      super(conf, args);
    }

    IInputSplit createSplit(Configuration conf, Object... args) {
      return (IInputSplit) Vars.inputSplit.applyTo(RT.cons(conf, RT.seq(args)));
    }
  }

  public static class RecordReader<K, V> extends ProxyRecordReader<K, V> {
    public RecordReader(org.apache.hadoop.mapreduce.RecordReader<K, V> rr) {
      super(rr);
    }

    @Override
    public void initialize(
        org.apache.hadoop.mapreduce.InputSplit split,
        TaskAttemptContext context)
          throws IOException, InterruptedException {
      split = (org.apache.hadoop.mapreduce.InputSplit)
        RT.nth(((IDeref) split).deref(), 1);
      rr.initialize(split, context);
    }
  }

  public static class Mapper<K1, V1, K2, V2>
      extends org.apache.hadoop.mapreduce.Mapper<K1, V1, K2, V2> {
    public void run(Context context) {
      ((IFn) Vars.mapper.invoke(context.getConfiguration())).invoke(context);
    }
  }
}
