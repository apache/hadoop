package org.apache.hadoop.fs.azurebfs.utils;

public enum TracingContextFormat {
  SINGLE_ID_FORMAT,  // <client-req-id>

  ALL_ID_FORMAT,  // <client-correlation-id>:<client-req-id>:<filesystem-id>
  // :<primary-req-id>:<stream-id>:<hdfs-operation>:<retry-count>

  TWO_ID_FORMAT; // <correlation-id>:<client-req-id>
}
