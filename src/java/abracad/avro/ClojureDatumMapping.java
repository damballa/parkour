package abracad.avro;

import clojure.lang.RT;
import clojure.lang.Symbol;
import clojure.lang.Var;
import clojure.lang.IFn;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.DatumMapping;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class ClojureDatumMapping implements DatumMapping<Object> {

private static class Vars {
    private static final String NS = "abracad.avro.compare";
    private static final Var compare = RT.var(NS, "compare");
    static {
        RT.var("clojure.core", "require").invoke(Symbol.intern(NS));
    }
}

public DatumReader<Object>
createReader(Schema schema) {
    return createReader(schema, schema);
}

public DatumReader<Object>
createReader(Schema writerSchema, Schema readerSchema) {
    readerSchema = null != readerSchema ? readerSchema : writerSchema;
    return new ClojureDatumReader(writerSchema, readerSchema);
}

public DatumWriter<Object>
createWriter(Schema writerSchema) {
    return new ClojureDatumWriter(writerSchema);
}

public
int compare(Object x, Object y, Schema schema) {
    return RT.intCast(Vars.compare.invoke(x, y, schema));
}

}
