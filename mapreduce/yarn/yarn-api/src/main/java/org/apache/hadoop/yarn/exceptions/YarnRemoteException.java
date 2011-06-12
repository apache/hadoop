package org.apache.hadoop.yarn.exceptions;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

public abstract class YarnRemoteException extends IOException {
  private static final long serialVersionUID = 1L;
  
  public YarnRemoteException() {
    super();
  }
  
  public YarnRemoteException(String message, Throwable cause) {
    super(message, cause);
  }
  
  public YarnRemoteException(Throwable cause) {
    super(cause);
  }
  
  public YarnRemoteException(String message) {
    super(message);
  }
  
  @Override
  public void printStackTrace(PrintWriter pw) {
    pw.append("RemoteTrace: \n").append(getRemoteTrace())
      .append(" at LocalTrace: \n\t");
    super.printStackTrace(pw);
  }

  @Override
  public void printStackTrace(PrintStream ps) {
    ps.append("RemoteTrace: \n").append(getRemoteTrace())
      .append(" at Local Trace: \n\t");
    super.printStackTrace(ps);
  }
  
  public abstract String getRemoteTrace();
  
  public abstract YarnRemoteException getCause();
}