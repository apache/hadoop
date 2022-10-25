package org.apache.hadoop.fs.azurebfs.utils;

public enum MetricFormat {
  INTERNAL_BACKOFF_METRIC_FORMAT, // <client-correlation-id>:<client-req-id>:<filesystem-id>
  // :<backoff-metric-results>

  INTERNAL_FOOTER_METRIC_FORMAT,  // <client-correlation-id>:<client-req-id>:<filesystem-id>
  // :<footer-metric-results>

  INTERNAL_METRIC_FORMAT, // <client-correlation-id>:<client-req-id>:<filesystem-id>
  // :<backoff-metric-results>:<footer-metric-results>

  EMPTY;

  @Override
  public String toString() {
    return this == EMPTY ? "" : this.name();
  }
}
