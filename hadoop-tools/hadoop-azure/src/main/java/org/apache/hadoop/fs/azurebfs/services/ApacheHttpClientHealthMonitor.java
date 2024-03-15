package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.atomic.AtomicLong;

public class ApacheHttpClientHealthMonitor {

  private static final RollingWindow SERVER_CALLS = new RollingWindow(10);

  private static final RollingWindow IO_EXCEPTIONS = new RollingWindow(10);
  private ApacheHttpClientHealthMonitor() {}

  public static boolean usable() {
    return (double) (IO_EXCEPTIONS.getSum() / SERVER_CALLS.getSum()) < 0.01;

  }

  public static void incrementServerCalls() {
    SERVER_CALLS.add(1);
  }

  public static void incrementUnknownIoExceptions() {
    IO_EXCEPTIONS.add(1);
  }
}
