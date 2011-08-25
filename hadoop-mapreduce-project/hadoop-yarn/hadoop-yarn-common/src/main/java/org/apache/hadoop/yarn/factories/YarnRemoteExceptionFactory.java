package org.apache.hadoop.yarn.factories;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public interface YarnRemoteExceptionFactory {
  public YarnRemoteException createYarnRemoteException(String message);
  public YarnRemoteException createYarnRemoteException(String message, Throwable t);
  public YarnRemoteException createYarnRemoteException(Throwable t);
}
