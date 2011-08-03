package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
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
    NodeId node1 = rm.registerNode("h1", 5000);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    rm.nodeHeartbeat(node1, true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.registerAppAttempt(attempt.getAppAttemptId());
    
    //request for containers
    int request = 2;
    rm.allocate(attempt.getAppAttemptId(), "h1" , 1000, request, 
        new ArrayList<ContainerId>());
    
    //kick the scheduler
    rm.nodeHeartbeat(node1, true);
    List<Container> conts = rm.allocate(attempt.getAppAttemptId(), new ArrayList<Container>(),
        new ArrayList<ResourceRequest>());
    int contReceived = conts.size();
    while (contReceived < request) {
      conts = rm.allocate(attempt.getAppAttemptId(), new ArrayList<Container>(),
          new ArrayList<ResourceRequest>());
      contReceived += conts.size();
      Log.info("Got " + contReceived + " containers. Waiting to get " + request);
      Thread.sleep(2000);
    }
    Assert.assertEquals(request, conts.size());
    
    rm.unregisterAppAttempt(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.FINISHED);

    int size = rm.nodeHeartbeat(node1, true).getApplicationsToCleanupList().size();
    while(size < 1) {
      Thread.sleep(1000);
      Log.info("Waiting to get application cleanup..");
      size = rm.nodeHeartbeat(node1, true).getApplicationsToCleanupList().size();
    }
    Assert.assertEquals(1, size);

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestApplicationCleanup t = new TestApplicationCleanup();
    t.testAppCleanup();
  }
}