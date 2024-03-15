package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.atomic.AtomicLong;

public class ApacheHttpClientHealthMonitor {

  private static final RollingWindow SERVER_CALLS = new RollingWindow(10);

  private static final RollingWindow IO_EXCEPTIONS = new RollingWindow(10);
  private ApacheHttpClientHealthMonitor() {}

  public static boolean usable() {
    final long serverCalls = SERVER_CALLS.getSum();
    if(serverCalls == 0) {
      return true;
    }
    return (double) (IO_EXCEPTIONS.getSum() / serverCalls) < 0.01;

  }

  public static void incrementServerCalls() {
    SERVER_CALLS.add(1);
  }

  public static void incrementUnknownIoExceptions() {
    IO_EXCEPTIONS.add(1);
  }
}
