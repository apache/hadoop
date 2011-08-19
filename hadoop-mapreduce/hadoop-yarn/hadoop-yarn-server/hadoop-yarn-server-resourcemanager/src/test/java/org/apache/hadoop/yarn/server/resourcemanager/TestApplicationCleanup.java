package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestApplicationCleanup {

  @Test
  public void testAppCleanup() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();

    MockNM nm1 = rm.registerNode("h1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());
    am.registerAppAttempt();
    
    //request for containers
    int request = 2;
    am.allocate("h1" , 1000, request, 
        new ArrayList<ContainerId>());
    
    //kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getNewContainerList();
    int contReceived = conts.size();
    while (contReceived < request) {
      conts = am.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getNewContainerList();
      contReceived += conts.size();
      Log.info("Got " + contReceived + " containers. Waiting to get " + request);
      Thread.sleep(2000);
    }
    Assert.assertEquals(request, conts.size());
    
    am.unregisterAppAttempt();
    am.waitForState(RMAppAttemptState.FINISHED);

    int cleanedConts = 0;
    int cleanedApps = 0;
    List<ContainerId> contsToClean = null;
    List<ApplicationId> apps = null;
    
    //currently only containers are cleaned via this
    //AM container is cleaned via container launcher
    while (cleanedConts < 2 || cleanedApps < 1) {
      HeartbeatResponse resp = nm1.nodeHeartbeat(true);
      contsToClean = resp.getContainersToCleanupList();
      apps = resp.getApplicationsToCleanupList();
      Log.info("Waiting to get cleanup events.. cleanedConts: "
          + cleanedConts + " cleanedApps: " + cleanedApps);
      cleanedConts += contsToClean.size();
      cleanedApps += apps.size();
      Thread.sleep(1000);
    }
    
    Assert.assertEquals(1, apps.size());
    Assert.assertEquals(app.getApplicationId(), apps.get(0));

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestApplicationCleanup t = new TestApplicationCleanup();
    t.testAppCleanup();
  }
}