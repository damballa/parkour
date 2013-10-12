package parkour.hadoop;

import java.io.DataInput;
import java.io.DataOutput;

import clojure.lang.IDeref;

public interface IInputSplit extends IDeref {
  long getLength();
  String[] getLocations();
  IInputSplit readSplit(DataInput in);
  void write(DataOutput out);
}
