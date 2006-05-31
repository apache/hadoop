package org.apache.hadoop.dfs;

import java.io.IOException;

/**
 * The file has not finished being written to enough datanodes yet.
 * @author Owen O'Malley
 */
public class NotReplicatedYetException extends IOException {
  public NotReplicatedYetException(String msg) {
    super(msg);
  }
}
