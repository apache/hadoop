package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

public class MockRMApp implements RMApp {
  static final int DT = 1000000; // ms

  String user = MockApps.newUserName();
  String name = MockApps.newAppName();
  String queue = MockApps.newQueue();
  long start = System.currentTimeMillis() - (int) (Math.random() * DT);
  long finish = 0;
  RMAppState state = RMAppState.NEW;
  int failCount = 0;
  ApplicationId id;

  public MockRMApp(int newid, long time, RMAppState newState) {
    finish = time;
    id = MockApps.newAppID(newid);
    state = newState;
  }

  public MockRMApp(int newid, long time, RMAppState newState, String userName) {
    this(newid, time, newState);
    user = userName;
  }

  @Override
  public ApplicationId getApplicationId() {
    return id;
  }

  @Override
  public RMAppState getState() {
    return state;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public float getProgress() {
    return (float) 0.0;
  }

  @Override
  public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getQueue() {
    return queue;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public RMAppAttempt getCurrentAppAttempt() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ApplicationReport createAndGetApplicationReport() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public ApplicationStore getApplicationStore() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getFinishTime() {
    return finish;
  }

  @Override
  public long getStartTime() {
    return start;
  }

  @Override
  public String getTrackingUrl() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public StringBuilder getDiagnostics() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void handle(RMAppEvent event) {
  };

}
