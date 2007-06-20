package org.apache.hadoop.dfs;

import java.io.IOException;

/**
 * The lease that was being used to create this file has expired.
 */
public class LeaseExpiredException extends IOException {
  public LeaseExpiredException(String msg) {
    super(msg);
  }
}
