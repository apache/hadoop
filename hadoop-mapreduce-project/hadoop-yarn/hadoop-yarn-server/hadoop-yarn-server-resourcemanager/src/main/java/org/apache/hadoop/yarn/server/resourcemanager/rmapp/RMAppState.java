package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

public enum RMAppState {
  NEW, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED
}
