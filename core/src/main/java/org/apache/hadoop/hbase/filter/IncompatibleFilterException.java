package org.apache.hadoop.hbase.filter;

/**
 * Used to indicate a filter incompatibility
 */
public class IncompatibleFilterException extends RuntimeException {
  private static final long serialVersionUID = 3236763276623198231L;

/** constructor */
  public IncompatibleFilterException() {
    super();
  }

  /**
   * constructor
   * @param s message
   */
  public IncompatibleFilterException(String s) {
    super(s);
  }
}
