package org.apache.hadoop.mapreduce.v2.api.records;

public enum TaskAttemptCompletionEventStatus {
  FAILED,
  KILLED,
  SUCCEEDED,
  OBSOLETE,
  TIPFAILED
}
