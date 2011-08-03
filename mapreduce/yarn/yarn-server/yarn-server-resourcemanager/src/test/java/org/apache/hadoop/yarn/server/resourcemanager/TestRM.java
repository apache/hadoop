package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class TestRM {

  private static final Log LOG = LogFactory.getLog(TestRM.class);

  @Test
  public void testAppWithNoContainers() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    rm.registerNode("h1:1234", 5000);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    rm.nodeHeartbeat("h1:1234", true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.registerAppAttempt(attempt.getAppAttemptId());
    rm.unregisterAppAttempt(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.FINISHED);
    rm.stop();
  }

  @Test
  public void testAppOnMultiNode() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    rm.registerNode("h1:1234", 5000);
    rm.registerNode("h2:5678", 10000);
    
    RMApp app = rm.submitApp(2000);

    //kick the scheduling
    rm.nodeHeartbeat("h1:1234", true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    rm.sendAMLaunched(attempt.getAppAttemptId());
    rm.registerAppAttempt(attempt.getAppAttemptId());
    
    //request for containers
    int request = 13;
    rm.allocate(attempt.getAppAttemptId(), "h1" , 1000, request, 
        new ArrayList<ContainerId>());
    
    //kick the scheduler
    rm.nodeHeartbeat("h1:1234", true);
    List<Container> conts = rm.allocate(attempt.getAppAttemptId(), new ArrayList<Container>(),
        new ArrayList<ResourceRequest>());
    int contReceived = conts.size();
    while (contReceived < 3) {//only 3 containers are available on node1
      conts = rm.allocate(attempt.getAppAttemptId(), new ArrayList<Container>(),
          new ArrayList<ResourceRequest>());
      contReceived += conts.size();
      LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
      Thread.sleep(2000);
    }
    Assert.assertEquals(3, conts.size());

    //send node2 heartbeat
    rm.nodeHeartbeat("h2:5678", true);
    conts = rm.allocate(attempt.getAppAttemptId(), new ArrayList<Container>(),
        new ArrayList<ResourceRequest>());
    contReceived = conts.size();
    while (contReceived < 10) {
      conts = rm.allocate(attempt.getAppAttemptId(), new ArrayList<Container>(),
          new ArrayList<ResourceRequest>());
      contReceived += conts.size();
      LOG.info("Got " + contReceived + " containers. Waiting to get " + 10);
      Thread.sleep(2000);
    }
    Assert.assertEquals(10, conts.size());

    rm.unregisterAppAttempt(attempt.getAppAttemptId());
    rm.waitForState(attempt.getAppAttemptId(), RMAppAttemptState.FINISHED);

    rm.stop();
  }

  public static void main(String[] args) throws Exception {
    TestRM t = new TestRM();
    t.testAppWithNoContainers();
    t.testAppOnMultiNode();
  }
}
