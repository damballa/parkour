package parkour.hadoop;

import java.util.List;

import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.Symbol;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class InputFormats {
  private static class Base extends InputFormat {
    private static class Vars {
      private static final String NS = "parkour.remote.input";
      private static final Var getSplits = RT.var(NS, "get-splits");
      private static final Var
        createRecordReader = RT.var(NS, "create-record-reader");
      static {
        RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
      }
    }

    private final long id;

    Base() {
      super();
      this.id = Long.parseLong(getClass().getName().split("\\$_", 2)[1]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<InputSplit> getSplits(JobContext context) {
      return (List) Vars.getSplits.invoke(id, context);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RecordReader createRecordReader(
        InputSplit split, TaskAttemptContext context) {
      return (RecordReader) Vars.createRecordReader.invoke(id, split, context);
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
