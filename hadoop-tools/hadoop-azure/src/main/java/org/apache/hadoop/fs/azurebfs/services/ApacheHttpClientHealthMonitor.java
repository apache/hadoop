package org.apache.hadoop.fs.azurebfs.services;

public class ApacheHttpClientHealthMonitor {
  private ApacheHttpClientHealthMonitor() {}

  public static final ApacheHttpClientHealthMonitor HEALTH_MONITOR = new ApacheHttpClientHealthMonitor();

  public boolean usable() {
    return true;
  }
}
