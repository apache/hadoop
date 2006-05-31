package org.apache.hadoop.dfs;

import java.io.IOException;

/**
 * The lease that was being used to create this file has expired.
 * @author Owen O'Malley
 */
public class LeaseExpiredException extends IOException {
  public LeaseExpiredException(String msg) {
    super(msg);
  }
}
