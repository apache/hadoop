package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

public enum RMAppState {
  NEW, SUBMITTED, ACCEPTED, RUNNING, RESTARTING, FINISHED, FAILED, KILLED
}
