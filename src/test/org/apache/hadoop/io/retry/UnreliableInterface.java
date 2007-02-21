package org.apache.hadoop.io.retry;

public interface UnreliableInterface {
  
  public static class UnreliableException extends Exception {
    // no body
  }
  
  public static class FatalException extends UnreliableException {
    // no body
  }
  
  void alwaysSucceeds() throws UnreliableException;
  
  void alwaysfailsWithFatalException() throws FatalException;

  void failsOnceThenSucceeds() throws UnreliableException;
  boolean failsOnceThenSucceedsWithReturnValue() throws UnreliableException;

  void failsTenTimesThenSucceeds() throws UnreliableException;
}
