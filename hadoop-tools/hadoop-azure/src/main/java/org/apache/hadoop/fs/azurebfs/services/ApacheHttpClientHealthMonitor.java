package org.apache.hadoop.fs.azurebfs.services;

import java.util.concurrent.atomic.AtomicLong;

public class ApacheHttpClientHealthMonitor {

  private static final AtomicLong CONNECTION_RESETS = new AtomicLong(0);

  private static final AtomicLong SERVER_CALLS = new AtomicLong(0);

  private static final AtomicLong UNKNOWN_IOE_EXCEPTIONS = new AtomicLong(0);
  private ApacheHttpClientHealthMonitor() {}

  public static boolean usable() {
    return (double) ((CONNECTION_RESETS.get() + UNKNOWN_IOE_EXCEPTIONS.get())
        / SERVER_CALLS.get()) < 0.01;
  }

  public static void incrementConnectionResets() {
    CONNECTION_RESETS.incrementAndGet();
  }

  public static void incrementServerCalls() {
    SERVER_CALLS.incrementAndGet();
  }

  public static void incrementUnknownIoExceptions() {
    UNKNOWN_IOE_EXCEPTIONS.incrementAndGet();
  }
}
