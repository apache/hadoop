package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

public interface RMApp extends EventHandler<RMAppEvent>{

  ApplicationId getApplicationId();

  RMAppState getState();

  String getUser();

  float getProgress();

  RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId);

  String getQueue();

  String getName();

  RMAppAttempt getCurrentAppAttempt();

  ApplicationReport createAndGetApplicationReport();

  ApplicationStore getApplicationStore();

  long getFinishTime();

  long getStartTime();

  String getTrackingUrl();

  StringBuilder getDiagnostics();

}
