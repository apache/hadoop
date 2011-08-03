package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
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
    List<Container> conts = am.allocate(new ArrayList<Container>(),
        new ArrayList<ResourceRequest>());
    int contReceived = conts.size();
    while (contReceived < request) {
      conts = am.allocate(new ArrayList<Container>(),
          new ArrayList<ResourceRequest>());
      contReceived += conts.size();
      Log.info("Got " + contReceived + " containers. Waiting to get " + request);
      Thread.sleep(2000);
    }
    Assert.assertEquals(request, conts.size());
    
    am.unregisterAppAttempt();
    am.waitForState(RMAppAttemptState.FINISHED);

    int size = nm1.nodeHeartbeat(true).getApplicationsToCleanupList().size();
    while(size < 1) {
      Thread.sleep(1000);
      Log.info("Waiting to get application cleanup..");
      size = nm1.nodeHeartbeat(true).getApplicationsToCleanupList().size();
    }
    Assert.assertEquals(1, size);

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestApplicationCleanup t = new TestApplicationCleanup();
    t.testAppCleanup();
  }
}