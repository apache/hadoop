package org.apache.hadoop.mapreduce.v2.app2.rm.node;

public enum AMNodeState {
  ACTIVE,
  FORCED_ACTIVE,
  BLACKLISTED,
  UNHEALTHY,
}
