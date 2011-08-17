package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

public enum RMAppEventType {
  // Source: ClientRMService
  START,
  KILL,

  // Source: RMAppAttempt
  APP_REJECTED,
  APP_ACCEPTED,
  ATTEMPT_REGISTERED,
  ATTEMPT_FINISHED, // Will send the final state
  ATTEMPT_FAILED,
  ATTEMPT_KILLED
}
