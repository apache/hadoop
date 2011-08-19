package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;

public interface GetClusterMetricsResponse {
  public abstract YarnClusterMetrics getClusterMetrics();
  public abstract void setClusterMetrics(YarnClusterMetrics metrics);
}
