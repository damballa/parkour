package parkour.hadoop;

import java.io.Closeable;
import clojure.lang.Counted;
import clojure.lang.Seqable;

public interface RecordSeqable extends Closeable, Counted, Seqable {
}
