package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

public enum RMAppAttemptState {
  NEW, SUBMITTED, SCHEDULED, ALLOCATED, LAUNCHED, FAILED, RUNNING, FINISHED,
  KILLED,
}
