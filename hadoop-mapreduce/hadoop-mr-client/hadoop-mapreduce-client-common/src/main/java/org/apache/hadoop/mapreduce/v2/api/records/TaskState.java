package org.apache.hadoop.mapreduce.v2.api.records;

public enum TaskState {
  NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED
}
