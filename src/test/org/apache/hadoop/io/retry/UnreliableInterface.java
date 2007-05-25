package org.apache.hadoop.io.retry;

import org.apache.hadoop.ipc.RemoteException;

public interface UnreliableInterface {
  
  public static class UnreliableException extends Exception {
    // no body
  }
  
  public static class FatalException extends UnreliableException {
    // no body
  }
  
  void alwaysSucceeds() throws UnreliableException;
  
  void alwaysFailsWithFatalException() throws FatalException;
  void alwaysFailsWithRemoteFatalException() throws RemoteException;

  void failsOnceThenSucceeds() throws UnreliableException;
  boolean failsOnceThenSucceedsWithReturnValue() throws UnreliableException;

  void failsTenTimesThenSucceeds() throws UnreliableException;
}
