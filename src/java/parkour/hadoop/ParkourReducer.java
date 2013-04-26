package parkour.hadoop;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Symbol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class ParkourReducer extends Reducer {

public void
run(Context context) {
    Configuration conf = context.getConfiguration();
    String[] fqname = conf.get("parkour.reducer").split("/", 2);
    RT.var("clojure.core", "require").invoke(Symbol.intern(fqname[0]));
    ((IFn) RT.var(fqname[0], fqname[1]).invoke(conf)).invoke(context);
}

}
