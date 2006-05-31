package org.apache.hadoop.dfs;

import java.io.IOException;

/**
 * The exception that happens when you ask to create a file that already
 * is being created, but is not closed yet.
 * @author Owen O'Malley
 */
public class AlreadyBeingCreatedException extends IOException {
  public AlreadyBeingCreatedException(String msg) {
    super(msg);
  }
}
