package org.apache.hadoop.util.functional;

import java.io.IOException;

public interface Function3RaisingIOE<I1, I2, I3, I4> {

  void apply(I1 i1, I2 i2, I3 i3, I4 i4) throws IOException;
}
