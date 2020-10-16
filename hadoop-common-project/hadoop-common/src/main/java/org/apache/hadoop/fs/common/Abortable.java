package org.apache.hadoop.fs.common;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public interface Abortable {
  /*
   * Instruct the object to abort it's current transaction. This will also close
   * the object.
   */
  public void abort() throws IOException;
}
